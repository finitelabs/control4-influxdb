-- What was already under way when the license gate stops the logger: a write InfluxDB keeps refusing goes out no more,
-- its points wait and go out once a license is back, and a connection test answering after the stop leaves Driver
-- Status saying why.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_license_inflight.lua

require("c4_shim")
local T = require("testlib")
local cloud = require("dc_cloud")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

local posts = common.boot({ AC = 1, AS = "License Activated" }, 999)
local SENSOR, watts = common.SENSOR, 100

--- `n` rounds of 10 s: one ping, then every timer (the gate's read, the write intervals and their retries).
local function tick(n)
  for _ = 1, n or 1 do
    cloud.poll(1)
    ShimFireTimers()
  end
end

--- InfluxDB answering every request with `code`, or holding each answer until `release` when `code` is nil. Writes
--- go out through C4:urlPost and connection tests through lib.http's urlDo, to the same URL, so a write is recorded
--- as WRITE to tell it from a test.
local held = {}
local function answer(code)
  function C4:urlPost(url, _, _, _, callback)
    posts[#posts + 1] = "WRITE " .. tostring(url)
    if callback then
      callback(1, "", code or 204, {}, nil)
    end
    return 1
  end
  urlDo = function(method, url, _, _, callback)
    posts[#posts + 1] = tostring(method) .. " " .. tostring(url)
    if not callback then
      return
    end
    if code then
      callback(nil, code, {}, "", nil, url)
    else
      held[#held + 1] = function()
        callback(nil, 204, {}, "", nil, url)
      end
    end
  end
end
local function release()
  local due = held
  held = {}
  for _, fn in ipairs(due) do
    fn()
  end
end

--- Whether a write of points, not a connection test, went out after `mark`.
local function wrote(mark)
  for i = mark + 1, #posts do
    if posts[i]:find("^WRITE ") then
      return true
    end
  end
  return false
end

T.section("A write InfluxDB keeps refusing goes out no more once the logger stops, and goes out when a license is back")
do
  answer(503)
  watts = watts + 1
  OnWatchedVariableChanged(SENSOR, 1000, tostring(watts))
  local mark = #posts
  tick()
  T.eq("licensed, the refused write goes out and is retried", wrote(mark), true)
  cloud.answer = { AC = 0, AS = "Invalid License" }
  tick(30)
  T.eq("the 30th read stops it", Properties["Driver Status"], "No active license")
  mark = #posts
  tick(20)
  T.eq("stopped, the refused write is not retried", wrote(mark), false)
  answer(204)
  mark = #posts
  cloud.answer = { AC = 1, AS = "License Activated" }
  cloud.say({ AC = 1, AS = "License Activated" })
  T.eq("a license sends the points that waited, without waiting for a round", wrote(mark), true)
end

T.section("A connection test answering after the stop leaves Driver Status saying why")
do
  answer(nil)
  Properties["InfluxDB URL"] = "http://198.51.100.21:8181"
  OnPropertyChanged("InfluxDB URL")
  T.eq("the test is under way", #held > 0, true)
  cloud.answer = { AC = 0, AS = "Invalid License" }
  tick(30)
  T.eq("the 30th read stops it", Properties["Driver Status"], "No active license")
  release()
  T.eq("the late answer changes nothing", Properties["Driver Status"], "No active license")
end

T.finish()
