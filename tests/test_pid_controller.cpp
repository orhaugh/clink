// PidController unit tests.
//
// The autoscaler feeds queue-depth / saturation samples
// to a PidController; the controller's clamped output maps onto
// per-checkpoint scaling deltas. These tests pin the math + edge
// cases (saturation, anti-windup, reset, dt=0 safety) without
// touching any cluster / operator machinery.

#include <chrono>

#include <gtest/gtest.h>

#include "clink/async/pid_controller.hpp"

using clink::async::PidConfig;
using clink::async::PidController;
using namespace std::chrono_literals;

namespace {

// A controller tuned to drive the measured signal towards setpoint=1.0,
// emitting outputs in [-1, 1]. P-dominant so we see clear directional
// pressure on the first sample.
PidController make_default(double sp = 1.0) {
    PidController c({.kp = 1.0, .ki = 0.0, .kd = 0.0});
    c.set_setpoint(sp);
    return c;
}

}  // namespace

TEST(PidController, FirstSampleHonoursProportionalGain) {
    auto c = make_default(/*sp=*/1.0);
    // error = 1.0 - 0.5 = 0.5; output = kp * error = 0.5.
    const double out = c.update(0.5, 100ms);
    EXPECT_DOUBLE_EQ(out, 0.5);
}

TEST(PidController, OutputIsClampedToConfiguredRange) {
    PidController c({.kp = 10.0, .output_min = -1.0, .output_max = 1.0});
    c.set_setpoint(1.0);
    // Raw output = 10.0 * (1.0 - 0.0) = 10; clamped to 1.0.
    EXPECT_DOUBLE_EQ(c.update(0.0, 100ms), 1.0);
    // Negative direction symmetric.
    c.set_setpoint(0.0);
    EXPECT_DOUBLE_EQ(c.update(5.0, 100ms), -1.0);
}

TEST(PidController, IntegralTermAccumulatesOverTime) {
    PidController c({.kp = 0.0, .ki = 1.0, .output_min = -100.0, .output_max = 100.0});
    c.set_setpoint(1.0);
    // ki=1.0 with kp=0: pure integrator. After 3 samples of error=1.0
    // and dt=1s each, the integral should be 3.0 and output = ki*3 = 3.0.
    c.update(0.0, 1000ms);  // integral += 1 -> 1.0; out = 1.0
    c.update(0.0, 1000ms);  // integral += 1 -> 2.0; out = 2.0
    const double out = c.update(0.0, 1000ms);
    EXPECT_NEAR(out, 3.0, 1e-9);
    EXPECT_NEAR(c.integral(), 3.0, 1e-9);
}

TEST(PidController, AntiWindupHoldsIntegralWhileSaturated) {
    // Saturating output should freeze integral accumulation so the
    // controller can recover when the error finally drops. Without
    // anti-windup the integral grows unboundedly and the output
    // overshoots dramatically on the way back.
    PidController c({.kp = 0.0, .ki = 1.0, .output_min = -1.0, .output_max = 1.0});
    c.set_setpoint(1.0);
    // After the first sample the controller is already saturated
    // (ki * 1.0 = 1.0 hits output_max). Subsequent saturating
    // samples must NOT advance integral past its first-sample value.
    c.update(0.0, 1000ms);
    const double pinned_integral = c.integral();
    for (int i = 0; i < 5; ++i) {
        c.update(0.0, 1000ms);
    }
    EXPECT_NEAR(c.integral(), pinned_integral, 1e-9)
        << "anti-windup must freeze integral while saturated";
}

TEST(PidController, DerivativeTermRespondsToErrorRateOfChange) {
    // kd-only controller. error goes from 0 (measured=1.0) to
    // 0.5 (measured=0.5) over dt=1s; derivative = 0.5; output = kd*0.5.
    PidController c({.kp = 0.0, .ki = 0.0, .kd = 1.0, .output_min = -10.0, .output_max = 10.0});
    c.set_setpoint(1.0);
    c.update(1.0, 1000ms);  // first sample establishes prev_error=0
    const double out = c.update(0.5, 1000ms);
    EXPECT_NEAR(out, 0.5, 1e-9);
}

TEST(PidController, ZeroDtToleratedSafely) {
    // First sample with dt=0 should not divide-by-zero in the
    // derivative term. The controller falls back to the proportional
    // contribution only.
    auto c = make_default();
    const double out = c.update(0.0, 0ms);
    EXPECT_DOUBLE_EQ(out, 1.0);  // kp * (1.0 - 0.0)
    EXPECT_EQ(c.integral(), 0.0) << "dt=0 must not advance the integrator";
}

TEST(PidController, ResetClearsInternalState) {
    // Widen the output range past the default [-1, 1] so anti-windup
    // doesn't pin the integral at 0 during accumulation.
    PidController c({.kp = 0.0, .ki = 1.0, .output_min = -100.0, .output_max = 100.0});
    c.set_setpoint(1.0);
    c.update(0.0, 1000ms);  // integral = 1.0
    c.update(0.0, 1000ms);  // integral = 2.0
    EXPECT_NEAR(c.integral(), 2.0, 1e-9);
    c.reset();
    EXPECT_EQ(c.integral(), 0.0);
    EXPECT_EQ(c.output(), 0.0);
    // After reset, a fresh measurement should produce the same output
    // as a brand-new controller (proportional 0 + integral 0.5).
    const double after_reset = c.update(0.5, 1000ms);
    // First sample post-reset: error=0.5, integral becomes 0.5,
    // output = ki * 0.5 = 0.5.
    EXPECT_NEAR(after_reset, 0.5, 1e-9);
}

TEST(PidController, ConvergesUnderClosedLoopSimulation) {
    // Closed-loop sanity check: simulate a system where the "plant"
    // moves towards the controller's output. Over enough iterations
    // the measured value should converge to setpoint. Gentle kp + tiny
    // ki avoids overshoot in the toy plant.
    PidController c({.kp = 0.2, .ki = 0.05, .kd = 0.0});
    c.set_setpoint(1.0);
    double measured = 0.0;
    for (int i = 0; i < 200; ++i) {
        const double u = c.update(measured, 100ms);
        // Toy plant: measured drifts towards measured + u * 0.1 per step.
        measured += u * 0.1;
    }
    EXPECT_NEAR(measured, 1.0, 0.05) << "PID failed to converge to setpoint, got " << measured;
}
