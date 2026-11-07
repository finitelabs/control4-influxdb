-- Write Precision, in the real driver: each point goes out with its time in the unit the write asks InfluxDB to read
-- it in, including points buffered before the setting changed.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_write_precision.lua

require("c4_shim")
local T = require("testlib")
local cloud = require("dc_cloud")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

common.boot({ AC = 1, AS = "License Activated" }, 999)

local NOW_MS = 1794076298828
function C4:GetTime()
  return NOW_MS
end

local answer = { code = 204 }
local posted = {}
function C4:urlPost(url, data, _, _, callback)
  if tostring(url):find("/api/v2/write", 1, true) then
    posted[#posted + 1] = { url = tostring(url), body = tostring(data) }
  end
  if callback then
    callback(1, "", answer.code, {}, answer.err)
  end
  return 1
end

local watts = 100

--- A variable change and one round of timers.
local function write()
  watts = watts + 1
  OnWatchedVariableChanged(common.SENSOR, 1000, tostring(watts))
  cloud.poll(1)
  ShimFireTimers()
end

local function setPrecision(precision)
  Properties["Write Precision"] = precision
  OnPropertyChanged("Write Precision")
end

--- Every timestamp in the writes posted since `mark`, and the precision each write asked for.
local function sentSince(mark)
  local stamps, precisions = {}, {}
  for i = mark + 1, #posted do
    precisions[posted[i].url:match("precision=(%a+)")] = true
    for line in (posted[i].body .. "\n"):gmatch("([^\n]*)\n") do
      stamps[#stamps + 1] = line:match(" (%d+)$")
    end
  end
  return stamps, precisions
end

local function all(list, wanted)
  if #list == 0 then
    return false
  end
  for _, v in ipairs(list) do
    if v ~= wanted then
      return false
    end
  end
  return true
end

write()
T.eq("the driver starts Connected", Properties["Driver Status"], "Connected")

T.section("Each precision")
do
  local want = {
    s = "1794076298",
    ms = "1794076298828",
    us = "1794076298828000",
    ns = "1794076298828000000",
  }
  for _, precision in ipairs({ "s", "ms", "us", "ns" }) do
    setPrecision(precision)
    local mark = #posted
    write()
    local stamps, precisions = sentSince(mark)
    T.truthy(precision .. ": the write asks for " .. precision, precisions[precision])
    T.truthy(precision .. ": every point's time is " .. want[precision], all(stamps, want[precision]), stamps[1])
  end
end

T.section("Points buffered before the setting changed")
do
  setPrecision("ms")
  answer = { code = 0, err = "Connection refused" }
  write()
  T.eq("the write fails", Properties["Driver Status"], "Disconnected")
  setPrecision("s")
  answer = { code = 204 }
  local mark = #posted
  write()
  local stamps = sentSince(mark)
  T.truthy("more than one point goes out", #stamps > 1, #stamps)
  T.truthy("each in seconds", all(stamps, "1794076298"), table.concat(stamps, " "))
end

T.finish()
