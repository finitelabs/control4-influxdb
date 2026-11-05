-- The DriverCentral build's license gate, driven by the real vendored cloud client against a fake Cloud driver
-- (test/dc_cloud.lua). A licensed logger writes; once the license has read 0 for 30 reads (five minutes) it collects,
-- writes and tests nothing, says so in Driver Status, and a license starts it again at once.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_license_gate.lua

require("c4_shim")
local T = require("testlib")
local cloud = require("dc_cloud")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

local posts = common.boot({ AC = 1, AS = "License Activated" }, 999)
local SENSOR, watts = common.SENSOR, 100

--- `n` rounds of 10 s: one ping, then every timer (the gate's read and the write intervals).
local function tick(n)
  for _ = 1, n or 1 do
    cloud.poll(1)
    ShimFireTimers()
  end
end

--- Whether a variable change and one round reach InfluxDB.
local function writes()
  local mark = #posts
  watts = watts + 1
  OnWatchedVariableChanged(SENSOR, 1000, tostring(watts))
  tick()
  return common.wrote(posts, mark)
end

T.section("A licensed logger writes, and keeps writing")
do
  T.eq("a variable change is written", writes(), true)
  tick(40)
  T.eq("still written 400 s later", writes(), true)
end

T.section("A lapsed license stops it after 30 reads, and nothing goes out while stopped")
do
  cloud.answer = { AC = 0, AS = "Invalid License" }
  tick(28)
  T.eq("29 reads of 0: still writing", writes(), true)
  tick()
  T.eq("the 30th read stops it", Properties["Driver Status"], "No active license")
  local mark = #posts
  T.eq("a variable change is not written", writes(), false)
  tick(5)
  Properties["InfluxDB URL"] = "http://198.51.100.20:8181"
  OnPropertyChanged("InfluxDB URL")
  T.eq("nothing at all goes out: no write, retry or connection test", #posts, mark)
  T.eq("Driver Status still says why", Properties["Driver Status"], "No active license")
end

T.section("A license starts it again at once")
do
  local mark = #posts
  cloud.answer = { AC = 1, AS = "License Activated" }
  cloud.say({ AC = 1, AS = "License Activated" })
  T.eq("the connection is tested without waiting for a read", #posts > mark, true)
  T.eq("Driver Status no longer says no license", Properties["Driver Status"] ~= "No active license", true)
  T.eq("and a variable change is written again", writes(), true)
end

T.finish()
