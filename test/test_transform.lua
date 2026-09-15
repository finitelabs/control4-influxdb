-- Tests for lib/transform.lua, specifically the non-finite result gate on
-- Transform.eval (DRV-122).
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_transform.lua

local T = require("testlib")

require("c4_shim")
require("lib.utils")

local Transform = require("lib.transform")

T.section("a transform that mints a non-finite value is caught, not written")

-- A finite input through a reasonable-looking expression can still produce a
-- non-finite: value/total is inf when total reads zero, math.log(0) is -inf.
-- The gate treats that like any other transform failure and falls back to the
-- raw value rather than letting nan/inf reach the write path.
do
  local result, err = Transform.eval("value / 0", "5")
  T.eq("value/0 falls back to the raw value", result, "5")
  T.check("and reports why", err ~= nil, err)
end

do
  local result, err = Transform.eval("math.log(0)", "5")
  T.eq("math.log(0) (-inf) falls back to the raw value", result, "5")
  T.check("and reports why", err ~= nil, err)
end

do
  local result = Transform.eval("value * 0 / 0", "5") -- NaN
  T.eq("a minted NaN falls back to the raw value", result, "5")
end

T.section("finite transform results pass through unchanged")

-- Controls in the other direction: the gate must not swallow a legitimate
-- finite result, including a legitimate zero or a large-but-finite number.
T.eq("multiplication", Transform.eval("value * 2", "5"), 10)
T.eq("division that stays finite", Transform.eval("value / 2", "5"), 2.5)
T.eq("an expression that yields zero", Transform.eval("value - value", "5"), 0)
T.eq("a large finite result", Transform.eval("value * 1000000", "5"), 5000000)
T.eq("a passthrough of the raw value", Transform.eval("value", "5"), 5)

T.section("a non-numeric transform result is unaffected by the numeric gate")

-- The gate only inspects numbers, so string/boolean results are untouched.
T.eq("a string result passes through", Transform.eval('"warm"', "5"), "warm")

T.finish()
