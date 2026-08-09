-- Tests that variables Director never notifies us about are re-read at enqueue
-- time instead of being logged at whatever value seeded them. Regression test
-- for DRV-95.
--
-- The globals agent (device 100001) accepts a variable listener and then never
-- calls back, neither synchronously at registration nor when the value changes.
-- Measured on a CORE 1: registering on any other item, including every ordinary
-- agent and system agent 100124, delivers the current value during the
-- RegisterVariableListener call, and 100001 is the lone item that stays silent.
-- Before this fix the cache seeded at subscribe time was the only value that
-- ever reached InfluxDB, so a threshold retuned in Composer kept logging its
-- startup value for the driver's lifetime.
--
-- DIRECTOR below models exactly that split: `pushes = true` items behave like
-- real devices, `pushes = false` items behave like 100001.
--
-- Run from the driver root:
--   make test
-- or:
--   LUA_PATH="$PWD/test/?.lua;$PWD/src/?.lua;$PWD/src/?/init.lua;$PWD/vendor/?.lua;$PWD/vendor/?/init.lua;;" \
--     luajit -e "require('c4_shim')" test/test_subscriptions.lua

local pass, fail = 0, 0
local function check(name, ok, detail)
  if ok then
    pass = pass + 1
    print(string.format("  ok   %s", name))
  else
    fail = fail + 1
    print(string.format("  FAIL %s%s", name, detail and ("  -> " .. tostring(detail)) or ""))
  end
end

---------------------------------------------------------------------------
-- A fake Director
---------------------------------------------------------------------------

--- Two items, identical except for whether Director notifies listeners.
--- 100001 is the globals agent; 500 stands in for any ordinary device.
local DIRECTOR = {
  [500] = {
    name = "Bathroom Sensor",
    pushes = true,
    vars = { [1001] = { name = "HUMIDITY", value = "41" } },
  },
  [100001] = {
    name = "variables",
    pushes = false,
    vars = {
      [2301] = { name = "MB_HIGH_HUMIDITY_DIFF_THRESHOLD", value = "4" },
      [2084] = { name = "MB_HIGH_HUMIDITY_VIOLATIONS_COUNT", value = "0" },
    },
  },
}

--- deviceId -> variableId -> callback, as Director would hold them.
local listeners = {}

--- Count of C4:GetDeviceVariable calls, to show what the re-read actually costs
--- in calls per tick rather than asserting only on the value.
local reads = 0

C4 = C4 or {}

function C4:GetDeviceVariable(deviceId, variableId)
  reads = reads + 1
  local item = DIRECTOR[deviceId]
  local var = item and item.vars[variableId]
  return var and var.value or nil
end

function C4:GetDeviceVariables(deviceId)
  local item = DIRECTOR[deviceId]
  if not item then
    return nil
  end
  local out = {}
  for id, var in pairs(item.vars) do
    out[tostring(id)] = { name = var.name, value = var.value, deviceid = deviceId }
  end
  return out
end

function C4:GetDeviceDisplayName(deviceId)
  local item = DIRECTOR[deviceId]
  return item and item.name or ""
end

--- Set a value the way Composer or a programming action would, and notify only
--- if the owning item is one Director actually pushes for.
local function directorSet(deviceId, variableId, value)
  DIRECTOR[deviceId].vars[variableId].value = value
  local cb = DIRECTOR[deviceId].pushes and listeners[deviceId] and listeners[deviceId][variableId]
  if cb then
    cb(deviceId, variableId, value)
  end
end

-- The drivers-common-public wrappers the engine calls. The real one hands the
-- registration straight to C4:RegisterVariableListener with no filtering, and
-- Director answers with the current value during that call.
function RegisterVariableListener(deviceId, variableId, callback)
  listeners[deviceId] = listeners[deviceId] or {}
  listeners[deviceId][variableId] = callback
  local item = DIRECTOR[deviceId]
  if item and item.pushes then
    callback(deviceId, variableId, item.vars[variableId].value)
  end
end

function UnregisterVariableListener(deviceId, variableId)
  if listeners[deviceId] then
    listeners[deviceId][variableId] = nil
  end
end

---------------------------------------------------------------------------
-- Fixture
---------------------------------------------------------------------------

local SubscriptionEngine = require("lib.subscriptions")

--- Points the engine handed to InfluxWriter, newest last.
local enqueued = {}
local fakeWriter = {
  enqueue = function(_, measName, tags, fields, opts, timestampMs)
    enqueued[#enqueued + 1] = { meas = measName, tags = tags, fields = fields, ts = timestampMs }
  end,
}

local MEASUREMENTS = {
  fan_logic = {
    enabled = true,
    interval = "Default",
    fieldDefs = { "humidity", "threshold", "violations" },
    tagDefs = {},
    readings = {
      ["Master Bathroom"] = {
        enabled = true,
        mappings = {
          humidity = { source = "variable", varId = "500:1001" },
          threshold = { source = "variable", varId = "100001:2301" },
          violations = { source = "variable", varId = "100001:2084" },
        },
      },
    },
  },
}

local function newEngine()
  enqueued = {}
  listeners = {}
  reads = 0
  return SubscriptionEngine:new({
    getMeasurements = function()
      return MEASUREMENTS
    end,
    getInfluxWriter = function()
      return fakeWriter
    end,
    getWriteInterval = function()
      return 10
    end,
  })
end

--- Value of one field in the most recently enqueued point.
local function lastField(name)
  local point = enqueued[#enqueued]
  local field = point and point.fields[name]
  return field and field.value or nil
end

--- Read the pushless flag without asserting the table exists, so a build
--- without the fix fails on the stale values it logs rather than blowing up on
--- a missing field before reaching them.
local function flagged(engine, deviceId, variableId)
  local perDevice = engine._pushless and engine._pushless[deviceId]
  return perDevice ~= nil and perDevice[variableId] == true
end

--- Whether any flag survives for a device, nil-safe the same way.
local function anyFlag(engine, deviceId)
  local perDevice = engine._pushless and engine._pushless[deviceId]
  return perDevice ~= nil and next(perDevice) ~= nil
end

---------------------------------------------------------------------------
-- Tests
---------------------------------------------------------------------------

print("Pushless variables are re-read at enqueue time (DRV-95)")

local engine = newEngine()
engine:subscribeToReading("fan_logic", "Master Bathroom")

check(
  "a silent registration is flagged pushless",
  flagged(engine, 100001, 2301) and flagged(engine, 100001, 2084),
  "no flag recorded for 100001"
)
check("a device that pushes is not flagged", not anyFlag(engine, 500), "device 500 was flagged pushless")
check(
  "seeding still fills the cache, so no field blocks its reading",
  engine._varCache[100001][2301].value == "4" and engine._varCache[500][1001].value == "41",
  "cache after subscribe"
)

-- Retune the threshold in Composer and let the violation count tick, the way
-- the fan logic does, then take the next sample.
directorSet(100001, 2301, "2")
directorSet(100001, 2084, "3")
directorSet(500, 1001, "57")

engine:_enqueueReadingPoint("fan_logic", "Master Bathroom")

check("the pushed device variable is current", lastField("humidity") == "57", lastField("humidity"))
check(
  "the retuned threshold is logged live, not at its startup value",
  lastField("threshold") == "2",
  lastField("threshold")
)
check(
  "the incrementing count is logged live, not at its startup value",
  lastField("violations") == "3",
  lastField("violations")
)

-- The re-read has to keep the cache honest, or the next consumer of _varCache
-- reads the stale value the enqueue path just worked around.
check(
  "the re-read writes back through the cache",
  engine._varCache[100001][2301].value == "2",
  engine._varCache[100001][2301].value
)

-- One read per pushless mapping per tick, and none for the pushed device.
reads = 0
engine:_enqueueReadingPoint("fan_logic", "Master Bathroom")
check("only pushless mappings are re-read", reads == 2, "reads = " .. tostring(reads))

-- A second sample with nothing changed must still track, since the whole bug
-- was a value that stayed put across ticks.
directorSet(100001, 2301, "6")
engine:_enqueueReadingPoint("fan_logic", "Master Bathroom")
check("later changes keep tracking", lastField("threshold") == "6", lastField("threshold"))

-- Unsubscribing has to drop the verdict along with the cache entry, or a
-- re-subscribe inherits a flag for a listener that no longer exists.
engine:_cleanupVariable("100001:2301", 100001, 2301)
check("cleanup clears the pushless flag", not flagged(engine, 100001, 2301), "flag survived cleanup")
check("cleanup leaves the other flag alone", flagged(engine, 100001, 2084), "sibling flag was dropped")

engine:_cleanupVariable("100001:2084", 100001, 2084)
check(
  "the device entry is dropped once its last flag goes",
  engine._pushless and engine._pushless[100001] == nil,
  "empty device table left behind"
)

-- Removing the device entirely takes the whole table with it.
local engine2 = newEngine()
engine2:subscribeToReading("fan_logic", "Master Bathroom")
engine2:handleDeviceRemoved(100001)
check("handleDeviceRemoved drops every flag for the device", not anyFlag(engine2, 100001), "flags remain")

print(string.format("\n%d passed, %d failed", pass, fail))
os.exit(fail == 0 and 0 or 1)
