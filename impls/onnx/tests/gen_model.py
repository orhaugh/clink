#!/usr/bin/env python3
# Regenerates onnx_test_model.hpp - the tiny ONNX fixtures the provider tests run.
#
# Four models exercise the provider's I/O paths (all IR version 9 so ONNX Runtime
# 1.20.x accepts them; small, deterministic, no external file at build time):
#   kOnnxAddModel    x:float32[1,2]                 -> y:float32[1,2] = x + [10,20]
#                    (single float input, positional float outputs)
#   kOnnxArgmaxModel x:float32[1,3]                 -> label:int64[1] = argmax(x),
#                                                      score:float32[1] = max(x)
#                    (named, mixed-dtype outputs read by column name)
#   kOnnxStrIdModel  s:string[1]                    -> out:string[1] = s
#                    (string input + string output)
#   kOnnxAdd2Model   a:float32[1,1], b:float32[1,1] -> sum:float32[1,1] = a + b
#                    (named multi-input)
#
# Usage (needs `pip install onnx`):  python3 gen_model.py
import os

import numpy as np
import onnx
from onnx import TensorProto, helper, numpy_helper


def finalize(graph):
    m = helper.make_model(
        graph, producer_name="clink-test", opset_imports=[helper.make_opsetid("", 13)]
    )
    m.ir_version = 9  # ORT 1.20.x accepts it; a newer onnx would stamp a rejected version
    onnx.checker.check_model(m)
    return m.SerializeToString()


def add_model():
    bias = numpy_helper.from_array(np.array([10.0, 20.0], dtype=np.float32), name="bias")
    node = helper.make_node("Add", ["x", "bias"], ["y"], name="add")
    g = helper.make_graph(
        [node], "clink_add",
        [helper.make_tensor_value_info("x", TensorProto.FLOAT, [1, 2])],
        [helper.make_tensor_value_info("y", TensorProto.FLOAT, [1, 2])],
        initializer=[bias],
    )
    return finalize(g)


def argmax_model():
    # label = argmax(x) over axis 1 (int64[1]); score = max(x) over axis 1 (float32[1]).
    amax = helper.make_node("ArgMax", ["x"], ["label"], name="amax", axis=1, keepdims=0)
    rmax = helper.make_node("ReduceMax", ["x"], ["score"], name="rmax", axes=[1], keepdims=0)
    g = helper.make_graph(
        [amax, rmax], "clink_argmax",
        [helper.make_tensor_value_info("x", TensorProto.FLOAT, [1, 3])],
        [
            helper.make_tensor_value_info("label", TensorProto.INT64, [1]),
            helper.make_tensor_value_info("score", TensorProto.FLOAT, [1]),
        ],
    )
    return finalize(g)


def strid_model():
    node = helper.make_node("Identity", ["s"], ["out"], name="id")
    g = helper.make_graph(
        [node], "clink_strid",
        [helper.make_tensor_value_info("s", TensorProto.STRING, [1])],
        [helper.make_tensor_value_info("out", TensorProto.STRING, [1])],
    )
    return finalize(g)


def add2_model():
    node = helper.make_node("Add", ["a", "b"], ["sum"], name="add2")
    g = helper.make_graph(
        [node], "clink_add2",
        [
            helper.make_tensor_value_info("a", TensorProto.FLOAT, [1, 1]),
            helper.make_tensor_value_info("b", TensorProto.FLOAT, [1, 1]),
        ],
        [helper.make_tensor_value_info("sum", TensorProto.FLOAT, [1, 1])],
    )
    return finalize(g)


def emit_bytes(out, name, data):
    out.append(f"inline constexpr unsigned char {name}[] = {{")
    line = "    "
    for b in data:
        line += f"{b},"
        if len(line) >= 96:
            out.append(line)
            line = "    "
    if line.strip():
        out.append(line)
    out.append("};")
    out.append(f"inline constexpr std::size_t {name}Len = {len(data)};")


models = [
    ("kOnnxAddModel", add_model()),
    ("kOnnxArgmaxModel", argmax_model()),
    ("kOnnxStrIdModel", strid_model()),
    ("kOnnxAdd2Model", add2_model()),
]

out = [
    "// Generated ONNX test fixtures for the model provider. Regenerate with gen_model.py",
    "// (in this directory) if the model surface changes. Do not hand-edit.",
    "#pragma once",
    "#include <cstddef>",
    "namespace clink::onnx::test {",
]
for name, data in models:
    emit_bytes(out, name, data)
out.append("}  // namespace clink::onnx::test")

dst = os.path.join(os.path.dirname(os.path.abspath(__file__)), "onnx_test_model.hpp")
with open(dst, "w") as f:
    f.write("\n".join(out) + "\n")
print(f"wrote {dst}: " + ", ".join(f"{n}={len(d)}B" for n, d in models))
