-- The INFLUX_ variables' ids, in the real driver. Programming binds to a variable's id, so each keeps its id through
-- a driver update and a Director restart, also in a project that ran v20260816, which added its five variables in a
-- different order from this build's six.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_variable_ids.lua

require("c4_shim")
require("drivers-common-public.global.lib") -- Serialize
local T = require("testlib")
local cloud = require("dc_cloud")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

--- name -> id of every visible variable.
local function ids()
  local out = {}
  for id, variable in pairs(C4:GetDeviceVariables(C4:GetDeviceID())) do
    if variable.hidden ~= "True" then
      out[variable.name] = tonumber(id)
    end
  end
  return out
end

local watts = 100

--- A reading changes and its write goes out, which reports every INFLUX_ variable.
local function write()
  watts = watts + 1
  OnWatchedVariableChanged(common.SENSOR, 1000, tostring(watts))
  cloud.poll(1)
  ShimFireTimers()
end

T.section("A project that ran v20260816, updated to this build")
do
  -- What v20260816 left: its five variables, added by name in this order, and lib/values records that keep an
  -- index but no id.
  local old = {
    { "INFLUX_POINTS_BUFFERED", "INT", 0 },
    { "INFLUX_POINTS_WRITTEN", "INT", 0 },
    { "INFLUX_POINTS_DROPPED", "INT", 0 },
    { "INFLUX_WRITE_ERRORS", "INT", 0 },
    { "INFLUX_LAST_WRITE_TS", "STRING", "" },
  }
  local records = {}
  for i, v in ipairs(old) do
    C4:AddVariable(v[1], tostring(v[3]), v[2], true)
    records[v[1]] = { index = i, varType = v[2], value = v[3] }
  end
  C4:PersistSetValue("Values", Serialize(records))
  ShimUpdateDriver()

  common.boot({ AC = 1, AS = "License Activated" }, 999)
  write()
  local want = {
    INFLUX_POINTS_BUFFERED = 1001,
    INFLUX_POINTS_WRITTEN = 1002,
    INFLUX_POINTS_DROPPED = 1003,
    INFLUX_WRITE_ERRORS = 1004,
    INFLUX_LAST_WRITE_TS = 1005,
    INFLUX_WATCHDOG_FIRES = 1006,
  }
  T.eq("the five keep their ids and the new one comes after", ids(), want)

  common.restart(999)
  write()
  T.eq("after a Director restart, every id is the same", ids(), want)

  common.restart(999)
  write()
  T.eq("and after another", ids(), want)
end

T.section("A new install")
do
  ShimRestartDirector()
  C4:PersistDeleteValue("Values")
  common.restart(999)
  write()
  local first = ids()
  local count = 0
  for _ in pairs(first) do
    count = count + 1
  end
  T.eq("it has the six variables", count, 6)

  common.restart(999)
  write()
  T.eq("after a Director restart, every id is the same", ids(), first)
end

T.finish()
