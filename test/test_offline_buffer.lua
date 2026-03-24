--- Tests for lib/offline_buffer.lua
--- Run from repo root: lua test/test_offline_buffer.lua

-- Set up the package path to find our modules
local script_dir = debug.getinfo(1, "S").source:match("^@(.+)/[^/]+$") or "."
package.path = script_dir .. "/../src/?.lua;" .. script_dir .. "/../src/?/init.lua;" .. package.path

-- Load shim first (sets up C4 globals)
dofile(script_dir .. "/c4_shim.lua")

-- Minimal stubs for C4 functions used by modules
function UpdateProperty(name, value) end
function Serialize(v)
  if type(v) == "table" then
    -- very small serializer sufficient for tests
    local parts = {}
    for k, val in pairs(v) do
      if type(val) == "string" then
        parts[#parts + 1] = string.format('[%q]=%q', tostring(k), val)
      elseif type(val) == "number" then
        parts[#parts + 1] = string.format('[%q]=%s', tostring(k), tostring(val))
      elseif type(val) == "boolean" then
        parts[#parts + 1] = string.format('[%q]=%s', tostring(k), tostring(val))
      elseif type(val) == "table" then
        parts[#parts + 1] = string.format('[%q]=%s', tostring(k), Serialize(val))
      end
    end
    return "{" .. table.concat(parts, ",") .. "}"
  elseif type(v) == "string" then
    return string.format('%q', v)
  else
    return tostring(v)
  end
end
function Deserialize(s)
  if not s then return nil end
  local ok, result = pcall(load("return " .. s))
  if ok then return result end
  return nil
end
function TableDeepCopy(t)
  if type(t) ~= "table" then return t end
  local copy = {}
  for k, v in pairs(t) do
    copy[k] = TableDeepCopy(v)
  end
  return copy
end

-- Grab a reference to the persist module so we can flush its cache between tests
-- (persist is a singleton with internal caching; we need a clean slate per test)
local persist_mod = require("lib.persist")

local function resetPersist()
  -- Clear the in-memory backing store (used by PersistGetValue/PersistSetValue shims)
  for k in pairs(persist_store) do persist_store[k] = nil end
  -- Also clear the persist module's internal cache so it re-reads from the (now empty) store
  for k in pairs(persist_mod._persist) do persist_mod._persist[k] = nil end
end

-- Simple test harness
local passed = 0
local failed = 0

local function test(name, fn)
  resetPersist()
  local ok, err = pcall(fn)
  if ok then
    print("  PASS: " .. name)
    passed = passed + 1
  else
    print("  FAIL: " .. name .. "\n    " .. tostring(err))
    failed = failed + 1
  end
end

local function assert_eq(a, b, msg)
  if a ~= b then
    error(string.format("%s: expected %s, got %s", msg or "assertion failed", tostring(b), tostring(a)))
  end
end

local function assert_true(v, msg)
  if not v then
    error(msg or "expected true")
  end
end

-- ---------------------------------------------------------------
print("\n=== OfflineBuffer tests ===\n")

local OfflineBuffer = require("lib.offline_buffer")

-- Helper: create a fresh buffer with small limits for testing
local function newBuf(opts)
  return OfflineBuffer:new(opts)
end

-- ---------------------------------------------------------------
test("initial state is Disconnected", function()
  local b = newBuf()
  assert_eq(b:getState(), "Disconnected")
end)

test("initial size is 0", function()
  local b = newBuf()
  assert_eq(b:size(), 0)
end)

test("push stores points", function()
  local b = newBuf()
  b:push({"point1", "point2", "point3"})
  assert_eq(b:size(), 3)
end)

test("FIFO eviction when over max_points", function()
  local b = newBuf({ max_points = 5, max_bytes = 99999 })
  b:push({"a", "b", "c", "d", "e"})
  assert_eq(b:size(), 5)
  -- Push 2 more; oldest 2 should be evicted
  local evicted = b:push({"f", "g"}) or 0
  assert_eq(b:size(), 5)
  assert_eq(evicted, 2)
  -- Verify oldest were dropped (a and b)
  local buf = b:_load()
  assert_eq(buf[1], "c")
  assert_eq(buf[5], "g")
end)

test("clear empties the buffer", function()
  local b = newBuf()
  b:push({"x", "y", "z"})
  b:clear()
  assert_eq(b:size(), 0)
end)

test("state changes fire onStateChange callback", function()
  local b = newBuf()
  local last_state = nil
  b:setCallbacks(nil, function(state) last_state = state end, nil)
  b:_setState("Connected")
  assert_eq(last_state, "Connected")
  b:_setState("Disconnected")
  assert_eq(last_state, "Disconnected")
end)

test("setState does not fire callback when state unchanged", function()
  local b = newBuf()
  b:_setState("Disconnected")
  local count = 0
  b:setCallbacks(nil, function() count = count + 1 end, nil)
  b:_setState("Disconnected") -- same state
  assert_eq(count, 0)
end)

test("backoff starts at first schedule entry", function()
  local b = newBuf()
  assert_eq(b:_backoffDelay(), 5)
end)

test("backoff advances on failure", function()
  local b = newBuf()
  b:_advanceBackoff()
  assert_eq(b:_backoffDelay(), 15)
  b:_advanceBackoff()
  assert_eq(b:_backoffDelay(), 30)
end)

test("backoff caps at max schedule entry", function()
  local b = newBuf()
  for _ = 1, 20 do b:_advanceBackoff() end
  assert_eq(b:_backoffDelay(), 900)
end)

test("onWriteFailure with retriable=true buffers points and advances backoff", function()
  local b = newBuf()
  b:onWriteFailure(true, {"p1", "p2"})
  assert_eq(b:size(), 2)
  assert_eq(b._backoffIndex, 2)
  assert_eq(b:getState(), "Disconnected")
end)

test("onWriteFailure with retriable=false does not buffer points", function()
  local b = newBuf()
  b:onWriteFailure(false, {"p1", "p2"})
  assert_eq(b:size(), 0)
end)

test("onWriteSuccess removes delivered points", function()
  local b = newBuf()
  b:push({"a", "b", "c", "d", "e"})
  b:onWriteSuccess(3)
  -- Should have removed first 3
  assert_eq(b:size(), 2)
  local buf = b:_load()
  assert_eq(buf[1], "d")
  assert_eq(buf[2], "e")
end)

test("onWriteSuccess with full drain transitions to Connected", function()
  local b = newBuf()
  b:push({"a", "b"})
  -- Simulate the timer firing synchronously for this test
  b._retryTimerId = nil
  -- Override SetTimer to fire immediately
  local orig_set = C4.SetTimer
  C4.SetTimer = function(self, ms, cb, rep)
    -- Don't actually schedule; just return a stub
    return { Cancel = function() end }
  end
  b:onWriteSuccess(2)
  -- Buffer is now empty, state should be Connected
  assert_eq(b:getState(), "Connected")
  C4.SetTimer = orig_set
end)

test("outage threshold fires onOutage callback", function()
  local b = newBuf({ outage_threshold = 0 }) -- threshold of 0 = fires immediately
  b._disconnectedAt = os.time() - 1 -- 1 second ago
  local fired = false
  b:setCallbacks(nil, nil, function() fired = true end)
  b:_checkOutageThreshold()
  assert_true(fired, "outage callback should have fired")
end)

test("outage notification fires only once per outage", function()
  local b = newBuf({ outage_threshold = 0 })
  b._disconnectedAt = os.time() - 1
  local count = 0
  b:setCallbacks(nil, nil, function() count = count + 1 end)
  b:_checkOutageThreshold()
  b:_checkOutageThreshold()
  b:_checkOutageThreshold()
  assert_eq(count, 1)
end)

test("reconnecting to Connected resets disconnectedAt", function()
  local b = newBuf()
  b._disconnectedAt = os.time() - 100
  b:_setState("Reconnecting")
  b:_setState("Connected")
  assert_true(b._disconnectedAt == nil, "disconnectedAt should be nil after connecting")
end)

test("destroy cancels retry timer", function()
  local b = newBuf()
  local cancelled = false
  b._retryTimerId = { Cancel = function() cancelled = true end }
  b:destroy()
  assert_true(cancelled, "timer should be cancelled on destroy")
  assert_true(b._retryTimerId == nil, "retryTimerId should be nil after destroy")
end)

-- ---------------------------------------------------------------
print(string.format("\n%d passed, %d failed\n", passed, failed))
if failed > 0 then
  os.exit(1)
end
