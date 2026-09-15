-- Tests for lib/subscriptions.lua, specifically the non-finite drop guard at
-- the enqueue chokepoint (DRV-122).
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_subscriptions.lua

local T = require("testlib")

require("c4_shim")
require("lib.utils")
require("drivers-common-public.global.lib")
require("drivers-common-public.global.timer")
require("drivers-common-public.global.handlers")

local function case(name, fn)
  local ok, err = pcall(fn)
  if not ok then
    T.check(name, false, err)
  end
end

local Subscriptions = require("lib.subscriptions")

--- A writer stub that records every enqueue instead of batching or posting.
local function capturingWriter()
  local calls = {}
  return {
    enqueue = function(_, measName, tags, fields, opts, ts)
      calls[#calls + 1] = { meas = measName, tags = tags, fields = fields, opts = opts, ts = ts }
    end,
  },
    calls
end

--- Build an engine whose one measurement has the given field mappings, all
--- literal so no var cache is needed.
local function engineWith(fieldDefs, mappings, writer)
  return Subscriptions:new({
    getMeasurements = function()
      return {
        power = {
          enabled = true,
          fieldDefs = fieldDefs,
          tagDefs = {},
          interval = "Default",
          readings = { main = { enabled = true, mappings = mappings } },
        },
      }
    end,
    getInfluxWriter = function()
      return writer
    end,
    getWriteInterval = function()
      return 30
    end,
  })
end

T.section("a non-finite reading is dropped, its finite siblings still flush")

case("the bad field is dropped and the good field is still enqueued", function()
  local writer, calls = capturingWriter()
  local engine = engineWith({ "watts", "volts" }, {
    watts = { source = "literal", literal = "nan" },
    volts = { source = "literal", literal = "120" },
  }, writer)

  engine:_enqueueReadingPoint("power", "main", 1000)

  T.eq("the batch still flushed (enqueue was called once)", #calls, 1)
  local fields = calls[1] and calls[1].fields or {}
  T.check("the non-finite field was dropped", fields.watts == nil, fields.watts)
  T.check("the good sibling survived", fields.volts ~= nil)
  T.eq("and carries its real value", fields.volts and fields.volts.value, "120")
end)

case("infinity and overflow literals are dropped the same way", function()
  for _, bad in ipairs({ "inf", "-inf", "1e999" }) do
    local writer, calls = capturingWriter()
    local engine = engineWith({ "watts", "volts" }, {
      watts = { source = "literal", literal = bad },
      volts = { source = "literal", literal = "120" },
    }, writer)
    engine:_enqueueReadingPoint("power", "main", 1000)
    local fields = calls[1] and calls[1].fields or {}
    T.check(string.format("%q is dropped", bad), fields.watts == nil, fields.watts)
    T.check(string.format("its sibling survives %q", bad), fields.volts ~= nil)
  end
end)

case("a reading whose only field is non-finite enqueues nothing at all", function()
  local writer, calls = capturingWriter()
  local engine = engineWith({ "watts" }, {
    watts = { source = "literal", literal = "nan" },
  }, writer)
  engine:_enqueueReadingPoint("power", "main", 1000)
  T.eq("no point is enqueued when nothing valid remains", #calls, 0)
end)

T.section("the guard drops only non-finite numbers, not ordinary values")

case("a genuine non-numeric string field is kept, not dropped", function()
  local writer, calls = capturingWriter()
  local engine = engineWith({ "state" }, {
    state = { source = "literal", literal = "warm" },
  }, writer)
  engine:_enqueueReadingPoint("power", "main", 1000)
  T.eq("the reading still flushed", #calls, 1)
  local fields = calls[1] and calls[1].fields or {}
  T.check("the string field survived", fields.state ~= nil)
  T.eq("with its value intact", fields.state and fields.state.value, "warm")
end)

case("a finite numeric field is kept", function()
  local writer, calls = capturingWriter()
  local engine = engineWith({ "watts" }, {
    watts = { source = "literal", literal = "42.5" },
  }, writer)
  engine:_enqueueReadingPoint("power", "main", 1000)
  local fields = calls[1] and calls[1].fields or {}
  T.check("the finite field survived", fields.watts ~= nil)
  T.eq("with its value intact", fields.watts and fields.watts.value, "42.5")
end)

T.finish()
