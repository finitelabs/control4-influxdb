-- Driver Status and the Disconnected and Connected events through an outage, in the real driver: a write that can't
-- reach InfluxDB says so, and one that gets through says that.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_outage_status.lua

require("c4_shim")
local T = require("testlib")
local cloud = require("dc_cloud")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

common.boot({ AC = 1, AS = "License Activated" }, 999)

local events = {}
function C4:FireEvent(name)
  events[#events + 1] = name
end

local answer = { code = 204 }
function C4:urlPost(_, _, _, _, callback)
  if callback then
    callback(1, "", answer.code, {}, answer.err)
  end
  return 1
end

local watts = 100

--- A variable change and one round of 10 s.
local function write()
  watts = watts + 1
  OnWatchedVariableChanged(common.SENSOR, 1000, tostring(watts))
  cloud.poll(1)
  ShimFireTimers()
end

local function count(wanted)
  local n = 0
  for _, name in ipairs(events) do
    if name == wanted then
      n = n + 1
    end
  end
  return n
end

T.section("The server stops answering")
do
  write()
  T.eq("Driver Status starts Connected", Properties["Driver Status"], "Connected")
  events = {}
  answer = { code = 0, err = "Connection refused" }
  write()
  T.eq("it reads Disconnected", Properties["Driver Status"], "Disconnected")
  T.eq("Disconnected fires", count("Disconnected"), 1)
  T.eq("the conditional agrees", TC.INFLUXDB_CONNECTED(), false)
  for _ = 1, 5 do
    write()
  end
  T.eq("still Disconnected", Properties["Driver Status"], "Disconnected")
  T.eq("and the event fired only once", count("Disconnected"), 1)
end

T.section("The server comes back")
do
  answer = { code = 204 }
  for _ = 1, 3 do
    write()
  end
  T.eq("it reads Connected", Properties["Driver Status"], "Connected")
  T.eq("Connected fires once", count("Connected"), 1)
  T.eq("the conditional agrees", TC.INFLUXDB_CONNECTED(), true)
end

T.section("A rate limit")
do
  events = {}
  answer = { code = 429 }
  write()
  T.eq("still reads Connected: the server answered", Properties["Driver Status"], "Connected")
  T.eq("no Disconnected", count("Disconnected"), 0)
  T.check("a Write Error says it failed", count("Write Error") >= 1, count("Write Error"))
end

T.finish()
