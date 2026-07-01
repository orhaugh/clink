#!/usr/bin/env python3
# Regenerates onnx_test_model.hpp - the tiny ONNX fixture the provider test runs.
#
# The model is y = x + [10, 20] (float32[1,2] -> float32[1,2]): small, deterministic,
# and distinguishable from its input so the test proves the provider marshals features
# -> input tensor -> ORT run -> output columns (not an echo). The bytes are embedded in
# a committed header so the test needs no external file or Python at build time.
#
# Usage (needs `pip install onnx`):  python3 gen_model.py
import os

import numpy as np
import onnx
from onnx import TensorProto, helper, numpy_helper

bias = numpy_helper.from_array(np.array([10.0, 20.0], dtype=np.float32), name="bias")
node = helper.make_node("Add", inputs=["x", "bias"], outputs=["y"], name="add")
graph = helper.make_graph(
    [node],
    "clink_add_model",
    inputs=[helper.make_tensor_value_info("x", TensorProto.FLOAT, [1, 2])],
    outputs=[helper.make_tensor_value_info("y", TensorProto.FLOAT, [1, 2])],
    initializer=[bias],
)
model = helper.make_model(
    graph, producer_name="clink-test", opset_imports=[helper.make_opsetid("", 13)]
)
# Pin IR version 9: ONNX Runtime 1.20.x accepts it; a newer onnx would otherwise stamp
# an IR version older runtimes reject.
model.ir_version = 9
onnx.checker.check_model(model)
data = model.SerializeToString()

out = [
    "// Generated ONNX test fixture: y = x + [10, 20] (float32[1,2] -> float32[1,2]).",
    "// Regenerate with gen_model.py (in this directory) if the model surface changes.",
    "// Do not hand-edit.",
    "#pragma once",
    "#include <cstddef>",
    "namespace clink::onnx::test {",
    "inline constexpr unsigned char kOnnxAddModel[] = {",
]
line = "    "
for b in data:
    line += f"{b},"
    if len(line) >= 96:
        out.append(line)
        line = "    "
if line.strip():
    out.append(line)
out.append("};")
out.append(f"inline constexpr std::size_t kOnnxAddModelLen = {len(data)};")
out.append("}  // namespace clink::onnx::test")

dst = os.path.join(os.path.dirname(os.path.abspath(__file__)), "onnx_test_model.hpp")
with open(dst, "w") as f:
    f.write("\n".join(out) + "\n")
print(f"wrote {dst}, model bytes = {len(data)}")
