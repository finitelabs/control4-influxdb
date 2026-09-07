-- Tests for lib/offline_buffer.lua
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_offline_buffer.lua

local T = require("testlib")

local script_dir = debug.getinfo(1, "S").source:match("^@(.+)/[^/]+$") or "."
package.path = script_dir .. "/../src/?.lua;" .. script_dir .. "/../src/?/init.lua;" .. package.path

require("c4_shim")

-- Control4 exposes these as globals; the shim owns the C4 methods, not these.
function UpdateProperty(name, value) end -- luacheck: ignore

function Serialize(v) -- luacheck: ignore
  if type(v) == "table" then
    local parts = {}
    for k, val in pairs(v) do
      local kind = type(val)
      if kind == "string" then
        parts[#parts + 1] = string.format("[%q]=%q", tostring(k), val)
      elseif kind == "number" or kind == "boolean" then
        parts[#parts + 1] = string.format("[%q]=%s", tostring(k), tostring(val))
      elseif kind == "table" then
        parts[#parts + 1] = string.format("[%q]=%s", tostring(k), Serialize(val))
      end
    end
    return "{" .. table.concat(parts, ",") .. "}"
  elseif type(v) == "string" then
    return string.format("%q", v)
  end
  return tostring(v)
end

function Deserialize(s) -- luacheck: ignore
  if not s then
    return nil
  end
  local ok, result = pcall(load("return " .. s))
  if ok then
    return result
  end
  return nil
end

function TableDeepCopy(t) -- luacheck: ignore
  if type(t) ~= "table" then
    return t
  end
  local copy = {}
  for k, v in pairs(t) do
    copy[k] = TableDeepCopy(v)
  end
  return copy
end

local persist_mod = require("lib.persist")

--- persist is a singleton that caches, so a key written by one scenario would
--- still resolve in the next. Clear through the public API: the rendered shim
--- keeps its backing store module-local.
local function resetPersist()
  for k in pairs(persist_mod._persist) do
    C4:PersistDeleteValue(k)
  end
  persist_mod._persist = {}
end

--- One scenario against a clean persist store. testlib assertions do not raise,
--- so an error escaping fn is an unexpected crash rather than a failed
--- expectation, and has to be recorded or the scenario passes by vanishing.
local function case(name, fn)
  resetPersist()
  local ok, err = pcall(fn)
  if not ok then
    T.check(name, false, err)
  end
end

local OfflineBuffer = require("lib.offline_buffer")

local function newBuf(opts)
  return OfflineBuffer:new(opts)
end

T.section("Initial state")

case("initial state", function()
  T.eq("a fresh buffer starts Disconnected", newBuf():getState(), "Disconnected")
end)

case("initial size", function()
  T.eq("a fresh buffer holds no points", newBuf():size(), 0)
end)

T.section("Push and eviction")

case("push stores points", function()
  local b = newBuf()
  b:push({ "point1", "point2", "point3" })
  T.eq("size counts every pushed point", b:size(), 3)
end)

case("FIFO eviction when over max_points", function()
  local b = newBuf({ max_points = 5, max_bytes = 99999 })
  b:push({ "a", "b", "c", "d", "e" })
  T.eq("size holds at the cap when exactly full", b:size(), 5)

  local evicted = b:push({ "f", "g" }) or 0
  T.eq("size stays at the cap after overflow", b:size(), 5)
  T.eq("push reports the two evicted points", evicted, 2)

  local buf = b:_load()
  T.eq("the oldest surviving point is the third pushed", buf[1], "c")
  T.eq("the newest point is last", buf[5], "g")
end)

case("clear empties the buffer", function()
  local b = newBuf()
  b:push({ "x", "y", "z" })
  b:clear()
  T.eq("clear drops every point", b:size(), 0)
end)

T.section("State transitions")

case("state changes fire onStateChange", function()
  local b = newBuf()
  local last_state = nil
  b:setCallbacks(nil, function(state)
    last_state = state
  end, nil)

  b:_setState("Connected")
  T.eq("the callback sees Connected", last_state, "Connected")

  b:_setState("Disconnected")
  T.eq("the callback sees Disconnected", last_state, "Disconnected")
end)

case("no callback when state unchanged", function()
  local b = newBuf()
  b:_setState("Disconnected")
  local count = 0
  b:setCallbacks(nil, function()
    count = count + 1
  end, nil)
  b:_setState("Disconnected")
  T.eq("re-entering the current state fires nothing", count, 0)
end)

case("reconnecting to Connected resets disconnectedAt", function()
  local b = newBuf()
  b._disconnectedAt = os.time() - 100
  b:_setState("Reconnecting")
  b:_setState("Connected")
  T.eq("disconnectedAt clears once Connected", b._disconnectedAt, nil)
end)

T.section("Retry backoff")

case("backoff starts at the first schedule entry", function()
  T.eq("first delay is 5s", newBuf():_backoffDelay(), 5)
end)

case("backoff advances on failure", function()
  local b = newBuf()
  b:_advanceBackoff()
  T.eq("second delay is 15s", b:_backoffDelay(), 15)
  b:_advanceBackoff()
  T.eq("third delay is 30s", b:_backoffDelay(), 30)
end)

case("backoff caps at the last schedule entry", function()
  local b = newBuf()
  for _ = 1, 20 do
    b:_advanceBackoff()
  end
  T.eq("delay saturates at 900s", b:_backoffDelay(), 900)
end)

T.section("Write outcomes")

case("retriable failure buffers points and advances backoff", function()
  local b = newBuf()
  b:onWriteFailure(true, { "p1", "p2" })
  T.eq("the undelivered points are retained", b:size(), 2)
  T.eq("backoff advanced one step", b._backoffIndex, 2)
  T.eq("the buffer is Disconnected", b:getState(), "Disconnected")
end)

case("non-retriable failure discards points", function()
  local b = newBuf()
  b:onWriteFailure(false, { "p1", "p2" })
  T.eq("nothing is buffered for retry", b:size(), 0)
end)

case("onWriteSuccess removes delivered points", function()
  local b = newBuf()
  b:push({ "a", "b", "c", "d", "e" })
  b:onWriteSuccess(3)
  T.eq("only the undelivered points remain", b:size(), 2)

  local buf = b:_load()
  T.eq("the delivered prefix is gone", buf[1], "d")
  T.eq("the tail is intact", buf[2], "e")
end)

case("a full drain transitions to Connected", function()
  local b = newBuf()
  b:push({ "a", "b" })
  b._retryTimerId = nil

  local orig_set = C4.SetTimer
  C4.SetTimer = function()
    return { Cancel = function() end }
  end
  b:onWriteSuccess(2)
  C4.SetTimer = orig_set

  T.eq("draining the buffer marks it Connected", b:getState(), "Connected")
end)

T.section("Outage notification")

case("outage threshold fires onOutage", function()
  local b = newBuf({ outage_threshold = 0 })
  b._disconnectedAt = os.time() - 1
  local fired = false
  b:setCallbacks(nil, nil, function()
    fired = true
  end)
  b:_checkOutageThreshold()
  T.truthy("the outage callback fired", fired)
end)

case("outage notification fires once per outage", function()
  local b = newBuf({ outage_threshold = 0 })
  b._disconnectedAt = os.time() - 1
  local count = 0
  b:setCallbacks(nil, nil, function()
    count = count + 1
  end)
  b:_checkOutageThreshold()
  b:_checkOutageThreshold()
  b:_checkOutageThreshold()
  T.eq("repeated checks do not re-notify", count, 1)
end)

T.section("Teardown")

case("destroy cancels the retry timer", function()
  local b = newBuf()
  local cancelled = false
  b._retryTimerId = {
    Cancel = function()
      cancelled = true
    end,
  }
  b:destroy()
  T.truthy("the pending timer was cancelled", cancelled)
  T.eq("the timer handle is released", b._retryTimerId, nil)
end)

T.finish()
