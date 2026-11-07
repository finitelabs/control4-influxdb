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

-- The delay each outage clock starts with, and what it runs when it comes due.
local outageDelays, outageClocks = {}, {}
local driverSetTimer = SetTimer
SetTimer = function(name, ms, fn, repeating) -- luacheck: ignore
  if name == "InfluxExtendedOutage" then
    outageDelays[#outageDelays + 1] = ms
    outageClocks[#outageClocks + 1] = fn
  end
  return driverSetTimer(name, ms, fn, repeating)
end

T.section("A long outage")
do
  answer = { code = 204 }
  write()
  events = {}
  answer = { code = 0, err = "Connection refused" }
  write()
  T.eq("a failed write starts the clock at the threshold", outageDelays, { 300000 })
  T.eq("Extended Outage waits for it", count("Extended Outage"), 0)
  write()
  T.eq("then fires", count("Extended Outage"), 1)
  for _ = 1, 5 do
    write()
  end
  T.eq("once for the outage", count("Extended Outage"), 1)
  T.eq("and the clock doesn't start again", #outageDelays, 1)

  answer = { code = 204 }
  for _ = 1, 3 do
    write()
  end
  T.eq("the server is back", Properties["Driver Status"], "Connected")
  Properties["Outage Notification Threshold"] = "15m"
  OnPropertyChanged("Outage Notification Threshold")
  answer = { code = 0, err = "Connection refused" }
  write()
  T.eq("the next outage starts the clock at the new threshold", outageDelays[2], 900000)
  write()
  T.eq("and fires again", count("Extended Outage"), 2)
end

T.section("A failure that clears before the threshold")
do
  answer = { code = 204 }
  for _ = 1, 3 do
    write()
  end
  events = {}
  answer = { code = 0, err = "Connection refused" }
  write()
  T.eq("the clock starts", #outageDelays, 3)
  -- The server answers again, and the connection test after a settings change reports connected before the clock
  -- runs out.
  answer = { code = 204 }
  OnPropertyChanged("Database")
  T.eq("the test gets through", Properties["Driver Status"], "Connected")
  ShimFireTimers()
  T.eq("Extended Outage doesn't fire", count("Extended Outage"), 0)
end

T.section("A setting that isn't complete isn't an outage")
do
  local outages = #outageDelays
  answer = { code = 204 }
  for _ = 1, 3 do
    write()
  end
  events = {}
  Properties["InfluxDB URL"] = ""
  OnPropertyChanged("InfluxDB URL")
  T.eq("Driver Status says so", Properties["Driver Status"], "Not configured")
  for _ = 1, 3 do
    write()
  end
  T.eq("no clock starts", #outageDelays, outages)
  T.eq("and Extended Outage doesn't fire", count("Extended Outage"), 0)
end

T.section("A connection test that can't reach the server")
do
  local outages = #outageDelays
  local answeringUrlDo = urlDo
  urlDo = function(_, url, _, _, callback)
    callback("Connection refused", 0, {}, "", nil, url)
  end
  Properties["InfluxDB URL"] = "http://198.51.100.20:8181"
  OnPropertyChanged("InfluxDB URL")
  T.eq("Driver Status shows the test's answer", Properties["Driver Status"], "HTTP 0")
  T.eq("and the clock starts", #outageDelays, outages + 1)
  urlDo = answeringUrlDo
end

T.section("A license that lapses during an outage")
do
  answer = { code = 204 }
  for _ = 1, 3 do
    write()
  end
  events = {}
  local outages = #outageDelays
  cloud.answer = { AC = 0, AS = "Invalid License" }
  for _ = 1, 28 do
    write()
  end
  local answeringUrlDo = urlDo
  urlDo = function(_, url, _, _, callback)
    callback("Connection refused", 0, {}, "", nil, url)
  end
  answer = { code = 0, err = "Connection refused" }
  write()
  T.eq("the clock starts", #outageDelays, outages + 1)
  -- Held back, so it runs out after the driver stops.
  local clock = outageClocks[#outageClocks]
  CancelTimer("InfluxExtendedOutage")
  write()
  T.eq("the 30th read stops the driver", Properties["Driver Status"], "No active license")
  clock()
  T.eq("Extended Outage doesn't fire while it's stopped", count("Extended Outage"), 0)
  cloud.answer = { AC = 1, AS = "License Activated" }
  cloud.say({ AC = 1, AS = "License Activated" })
  T.eq("once licensed, the failing test starts the clock again", #outageDelays, outages + 2)
  write()
  T.eq("and it fires", count("Extended Outage"), 1)
  urlDo = answeringUrlDo
end

T.finish()
