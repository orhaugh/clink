"""Canonical cells (clink-canonical-cells-v1) from Python values.

A cell is text that equals another exactly when the two values are equal as a
column of the given type holds them:

    N            NULL
    I<n>         an integer, in decimal
    F<16 hex>    a Float64 by its IEEE-754 bits (f<8 hex>: a Float32); NaN is Fnan / fnan
    B0, B1       a Bool
    S<text>      a String
    D<n>@<s>     a Decimal as its unscaled integer at the column's scale
    T<n>@<p>     a DateTime64 as ticks of 10^-p s (DateTime: p = 0)
    d<n>         a Date as days since 1970-01-01

This is written apart from clink, from the rules above, so that the reference's
cells are not the job's code: a value the column cannot hold exactly raises
rather than being rounded, as it is the reference implementation's error.
"""

import datetime as _dt
import math
import re
import struct
from decimal import Decimal, InvalidOperation

_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
_INT_BITS = {"Int8": 8, "Int16": 16, "Int32": 32, "Int64": 64}
_UINT_BITS = {"UInt8": 8, "UInt16": 16, "UInt32": 32, "UInt64": 64}


class CellError(ValueError):
    """A value a column of the given type cannot hold exactly."""


class ColumnType:
    """A ClickHouse column type, as the scope document spells it."""

    def __init__(self, spelling):
        self.spelling = spelling
        t = spelling.strip()
        self.nullable = False
        m = re.fullmatch(r"Nullable\((.*)\)", t)
        if m:
            self.nullable, t = True, m.group(1).strip()
        m = re.fullmatch(r"LowCardinality\((.*)\)", t)
        if m:
            t = m.group(1).strip()
            m2 = re.fullmatch(r"Nullable\((.*)\)", t)
            if m2:
                self.nullable, t = True, m2.group(1).strip()
        self.scale = self.precision = 0
        if t in _INT_BITS:
            self.kind, self.bits = "int", _INT_BITS[t]
        elif t in _UINT_BITS:
            self.kind, self.bits = "uint", _UINT_BITS[t]
        elif t == "Float64":
            self.kind = "float64"
        elif t == "Float32":
            self.kind = "float32"
        elif t == "Bool":
            self.kind = "bool"
        elif t == "String":
            self.kind = "string"
        elif t in ("Date", "Date32"):
            self.kind = "date"
        elif t == "DateTime" or re.fullmatch(r"DateTime\('.*'\)", t):
            self.kind = "datetime"
        else:
            m = re.fullmatch(r"DateTime64\((\d+)(?:,\s*'.*')?\)", t)
            if m:
                self.kind, self.precision = "datetime64", int(m.group(1))
            else:
                m = re.fullmatch(r"Decimal\((\d+),\s*(\d+)\)", t)
                m32 = re.fullmatch(r"Decimal(32|64|128)\((\d+)\)", t)
                if m:
                    self.kind, self.precision, self.scale = "decimal", int(m.group(1)), int(m.group(2))
                elif m32:
                    digits = {"32": 9, "64": 18, "128": 38}[m32.group(1)]
                    self.kind, self.precision, self.scale = "decimal", digits, int(m32.group(2))
                else:
                    raise CellError("a column type this library does not encode: %s" % spelling)


def _ticks(value, precision):
    """Whole ticks of 10^-precision s since the epoch, exactly, or CellError."""
    if isinstance(value, _dt.datetime):
        when = value if value.tzinfo is not None else value.replace(tzinfo=_dt.timezone.utc)
        delta = when - _EPOCH
        micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
        if precision >= 6:
            return micros * 10 ** (precision - 6)
        step = 10 ** (6 - precision)
        if micros % step:
            raise CellError("%s is finer than %d decimal places of a second" % (value, precision))
        return micros // step
    if isinstance(value, int) and not isinstance(value, bool):
        return value  # already ticks at the column's precision
    raise CellError("a time must be a datetime or whole ticks, not %r" % (value,))


def cell(value, column_type):
    """The canonical cell of `value` in a column of `column_type` (a
    ColumnType or its spelling)."""
    t = column_type if isinstance(column_type, ColumnType) else ColumnType(column_type)
    if value is None:
        return "N"
    k = t.kind
    if k in ("int", "uint"):
        if isinstance(value, bool) or not isinstance(value, int):
            raise CellError("%s needs a whole number, not %r" % (t.spelling, value))
        lo, hi = ((-(1 << (t.bits - 1)), (1 << (t.bits - 1)) - 1) if k == "int"
                  else (0, (1 << t.bits) - 1))
        if not lo <= value <= hi:
            raise CellError("%d is outside %s" % (value, t.spelling))
        return "I%d" % value
    if k == "float64":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise CellError("%s needs a number, not %r" % (t.spelling, value))
        v = float(value)
        if math.isnan(v):
            return "Fnan"
        return "F%016x" % struct.unpack(">Q", struct.pack(">d", v))[0]
    if k == "float32":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise CellError("%s needs a number, not %r" % (t.spelling, value))
        v = float(value)
        if math.isnan(v):
            return "fnan"
        return "f%08x" % struct.unpack(">I", struct.pack(">f", v))[0]
    if k == "bool":
        if not isinstance(value, bool):
            raise CellError("%s needs True or False, not %r" % (t.spelling, value))
        return "B1" if value else "B0"
    if k == "string":
        if not isinstance(value, str):
            raise CellError("%s needs text, not %r" % (t.spelling, value))
        return "S" + value
    if k == "decimal":
        if isinstance(value, (bool, float)):
            raise CellError("%s needs an exact decimal (Decimal, int or text), not %r"
                            % (t.spelling, value))
        try:
            d = Decimal(value) if not isinstance(value, Decimal) else value
            scaled = d.scaleb(t.scale)
        except (InvalidOperation, TypeError, ValueError) as e:
            raise CellError("%r is not a decimal" % (value,)) from e
        if not d.is_finite() or scaled != scaled.to_integral_value():
            raise CellError("%s is not exact at scale %d" % (value, t.scale))
        unscaled = int(scaled)
        if len(str(abs(unscaled))) > t.precision:
            raise CellError("%s has more than %d digits" % (value, t.precision))
        return "D%d@%d" % (unscaled, t.scale)
    if k == "datetime64":
        return "T%d@%d" % (_ticks(value, t.precision), t.precision)
    if k == "datetime":
        return "T%d@0" % _ticks(value, 0)
    if k == "date":
        if isinstance(value, _dt.datetime):
            raise CellError("%s needs a date, not a datetime" % t.spelling)
        if isinstance(value, _dt.date):
            return "d%d" % (value - _dt.date(1970, 1, 1)).days
        if isinstance(value, int) and not isinstance(value, bool):
            return "d%d" % value
        raise CellError("%s needs a date or whole days, not %r" % (t.spelling, value))
    raise CellError("a column type this library does not encode: %s" % t.spelling)
