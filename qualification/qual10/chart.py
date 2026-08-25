#!/usr/bin/env python3
"""SVG line charts for QUAL-10, with no plotting dependency.

A leak campaign's verdict is a claim about SHAPE, and a reader who cannot
see the shape is being asked to take the arithmetic on trust. "RSS slope
0.02%/h" is a number that could mean anything; a flat line across twelve
hours, with the restarts marked on it, is the evidence itself.

Hand-rolled SVG rather than matplotlib because the rig and the laptop
should need nothing installed to produce the report, and because the output
has to survive as a committed artefact: an SVG is text, diffs, and renders
in GitHub and MkDocs without a runtime.

Restart markers are the reason this is not a generic plotting helper. A
process kill resets RSS, so an un-annotated series looks like a sawtooth
that a reader has to be TOLD is deliberate. Marking the kills lets the
sawtooth argue for itself, and makes the across-restart question - does
each plateau start where the last one did? - visible rather than asserted.
"""
import html

PALETTE = ["#2563eb", "#dc2626", "#059669", "#d97706",
           "#7c3aed", "#0891b2", "#be185d", "#4d7c0f"]


def _nice_bounds(lo, hi):
    """A y-range that does not exaggerate. A flat series auto-scaled to its
    own noise looks like a mountain range; padding to a sane band around it
    keeps 'flat' looking flat, which is the honest rendering."""
    if lo is None or hi is None:
        return 0.0, 1.0
    if hi <= lo:
        pad = abs(hi) * 0.1 or 1.0
        return lo - pad, hi + pad
    span = hi - lo
    return lo - span * 0.08, hi + span * 0.08


def line_chart(series, title, ylabel, events=None, width=900, height=340,
               y_zero=False, subtitle=""):
    """series: [{"label": str, "points": [(x_hours, y), ...]}]
    events:  [(x_hours, label)] - restarts, kills, phase boundaries."""
    series = [s for s in series if s.get("points")]
    if not series:
        return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
                f'height="60"><text x="10" y="35" font-family="system-ui" '
                f'font-size="14" fill="#666">{html.escape(title)}: no data</text></svg>')

    ml, mr, mt, mb = 78, 150, 46 if not subtitle else 60, 44
    pw, ph = width - ml - mr, height - mt - mb

    xs = [p[0] for s in series for p in s["points"]]
    ys = [p[1] for s in series for p in s["points"] if p[1] is not None]
    x0, x1 = min(xs), max(xs)
    if x1 <= x0:
        x1 = x0 + 1
    y0, y1 = _nice_bounds(min(ys) if ys else 0, max(ys) if ys else 1)
    if y_zero:
        y0 = 0.0

    def sx(x):
        return ml + (x - x0) / (x1 - x0) * pw

    def sy(y):
        return mt + ph - (y - y0) / (y1 - y0) * ph

    o = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
         f'height="{height}" viewBox="0 0 {width} {height}" '
         f'font-family="system-ui,-apple-system,Segoe UI,sans-serif">',
         f'<rect width="{width}" height="{height}" fill="#fff"/>',
         f'<text x="{ml}" y="24" font-size="15" font-weight="600" '
         f'fill="#111">{html.escape(title)}</text>']
    if subtitle:
        o.append(f'<text x="{ml}" y="42" font-size="12" fill="#666">'
                 f'{html.escape(subtitle)}</text>')

    # grid + y ticks
    for i in range(5):
        y = y0 + (y1 - y0) * i / 4
        yy = sy(y)
        o.append(f'<line x1="{ml}" y1="{yy:.1f}" x2="{ml + pw}" y2="{yy:.1f}" '
                 f'stroke="#e5e7eb" stroke-width="1"/>')
        o.append(f'<text x="{ml - 8}" y="{yy + 4:.1f}" font-size="11" '
                 f'fill="#6b7280" text-anchor="end">{_fmt(y)}</text>')
    # x ticks, in hours
    for i in range(7):
        x = x0 + (x1 - x0) * i / 6
        xx = sx(x)
        o.append(f'<line x1="{xx:.1f}" y1="{mt + ph}" x2="{xx:.1f}" '
                 f'y2="{mt + ph + 4}" stroke="#9ca3af"/>')
        o.append(f'<text x="{xx:.1f}" y="{mt + ph + 18}" font-size="11" '
                 f'fill="#6b7280" text-anchor="middle">{x:.1f}h</text>')
    o.append(f'<text x="{ml - 58}" y="{mt + ph / 2}" font-size="11" '
             f'fill="#374151" transform="rotate(-90 {ml - 58} {mt + ph / 2})" '
             f'text-anchor="middle">{html.escape(ylabel)}</text>')

    # restart / event markers, behind the data
    for ex, elabel in (events or []):
        if not (x0 <= ex <= x1):
            continue
        xx = sx(ex)
        o.append(f'<line x1="{xx:.1f}" y1="{mt}" x2="{xx:.1f}" y2="{mt + ph}" '
                 f'stroke="#f59e0b" stroke-width="1" stroke-dasharray="3,3" '
                 f'opacity="0.85"><title>{html.escape(elabel)}</title></line>')

    for i, s in enumerate(series):
        colour = PALETTE[i % len(PALETTE)]
        # A None breaks the line rather than being interpolated across: a
        # gap in the data must LOOK like a gap, not like a straight run
        # through the moment nobody was watching.
        run, paths = [], []
        for x, y in s["points"]:
            if y is None:
                if len(run) > 1:
                    paths.append(run)
                run = []
            else:
                run.append(f"{sx(x):.1f},{sy(y):.1f}")
        if len(run) > 1:
            paths.append(run)
        for pts in paths:
            o.append(f'<polyline fill="none" stroke="{colour}" '
                     f'stroke-width="1.6" points="{" ".join(pts)}"/>')
        ly = mt + 6 + i * 18
        o.append(f'<line x1="{ml + pw + 12}" y1="{ly}" x2="{ml + pw + 30}" '
                 f'y2="{ly}" stroke="{colour}" stroke-width="2.4"/>')
        o.append(f'<text x="{ml + pw + 35}" y="{ly + 4}" font-size="11" '
                 f'fill="#374151">{html.escape(s["label"])[:22]}</text>')

    if events:
        ly = mt + 6 + len(series) * 18 + 6
        o.append(f'<line x1="{ml + pw + 12}" y1="{ly}" x2="{ml + pw + 30}" '
                 f'y2="{ly}" stroke="#f59e0b" stroke-width="1.4" '
                 f'stroke-dasharray="3,3"/>')
        o.append(f'<text x="{ml + pw + 35}" y="{ly + 4}" font-size="11" '
                 f'fill="#6b7280">fault injected</text>')

    o.append(f'<rect x="{ml}" y="{mt}" width="{pw}" height="{ph}" '
             f'fill="none" stroke="#d1d5db"/>')
    o.append("</svg>")
    return "\n".join(o)


def _fmt(v):
    a = abs(v)
    if a >= 1e9:
        return f"{v / 1e9:.2f}G"
    if a >= 1e6:
        return f"{v / 1e6:.2f}M"
    if a >= 1e3:
        return f"{v / 1e3:.1f}k"
    if a >= 10:
        return f"{v:.0f}"
    return f"{v:.2f}"
