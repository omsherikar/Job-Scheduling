import { expect, test } from "vitest";
import { clock, duration, millis } from "./format";

test("duration reads in the largest useful unit", () => {
  expect(duration(45)).toBe("45s");
  expect(duration(90)).toBe("1m 30s");
  expect(duration(3660)).toBe("1h 1m");
  expect(duration(90000)).toBe("1d 1h");
});

test("missing values render as a dash rather than zero", () => {
  expect(duration(null)).toBe("-");
  expect(duration(undefined)).toBe("-");
  expect(millis(null)).toBe("-");
  expect(clock(null)).toBe("-");
});

test("millis switches to seconds past a thousand", () => {
  expect(millis(999)).toBe("999ms");
  expect(millis(1500)).toBe("1.5s");
});

test("clock renders utc without the T", () => {
  expect(clock("2026-08-23T14:32:37.123Z")).toBe("2026-08-23 14:32:37");
});
