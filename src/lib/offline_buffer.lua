--- Offline buffer with exponential backoff retry for InfluxDB writes.
---
--- When a write fails with a retriable error (5xx, 429, network error), points are
--- buffered to persistent storage. A reconnection timer periodically attempts to drain
--- the buffer, using exponential backoff to avoid hammering a down server.
---
--- ## Buffer storage layout
--- The buffer is stored as an array of line-protocol strings under the persist key
--- "offline_buffer". Buffer metadata (size, byte count) is stored separately.
---
--- ## Backoff schedule
--- Retry intervals: 5s, 15s, 30s, 60s, 300s, 900s (max).
--- The interval index advances each consecutive failure and resets on success.

local log = require("lib.logging")
local persist = require("lib.persist")

--- Persist keys used by this module.
--- @type table<string, string>
local PERSIST_KEYS = {
  BUFFER = "offline_buffer",
  META = "offline_buffer_meta",
  STATE = "connection_state",
}

--- Exponential backoff schedule in seconds.
--- @type number[]
local BACKOFF_SCHEDULE = { 5, 15, 30, 60, 300, 900 }

--- Connection state enum.
--- @enum ConnectionState
local ConnectionState = {
  CONNECTED = "Connected",
  DISCONNECTED = "Disconnected",
  RECONNECTING = "Reconnecting",
}

--- Default configuration.
local DEFAULTS = {
  MAX_POINTS = 10000,
  MAX_BYTES = 1024 * 1024, -- 1 MB
  OUTAGE_NOTIFICATION_THRESHOLD = 300, -- 5 minutes in seconds
}

--- @class OfflineBuffer
--- @field _config table Buffer configuration (max_points, max_bytes, outage_threshold)
--- @field _state string Current connection state
--- @field _backoffIndex number Current index into BACKOFF_SCHEDULE (1-based)
--- @field _retryTimerId number|nil Active retry timer ID
--- @field _disconnectedAt number|nil Timestamp (os.time) when we went disconnected
--- @field _outageNotified boolean Whether we've fired the extended-outage event this outage
--- @field _onFlush function|nil Callback: function(points) -> fires when buffer should drain
--- @field _onStateChange function|nil Callback: function(state) -> fires when connection state changes
--- @field _onOutage function|nil Callback: function() -> fires when outage exceeds threshold
local OfflineBuffer = {}
OfflineBuffer.__index = OfflineBuffer

--- Create a new OfflineBuffer instance.
--- @param opts? table Optional config overrides: max_points, max_bytes, outage_threshold
--- @return OfflineBuffer
function OfflineBuffer:new(opts)
  log:trace("OfflineBuffer:new()")
  opts = opts or {}
  local instance = setmetatable({}, self)
  instance._config = {
    max_points = opts.max_points or DEFAULTS.MAX_POINTS,
    max_bytes = opts.max_bytes or DEFAULTS.MAX_BYTES,
    outage_threshold = opts.outage_threshold or DEFAULTS.OUTAGE_NOTIFICATION_THRESHOLD,
  }
  instance._state = ConnectionState.DISCONNECTED
  instance._backoffIndex = 1
  instance._retryTimerId = nil
  instance._disconnectedAt = nil
  instance._outageNotified = false
  instance._onFlush = nil
  instance._onStateChange = nil
  instance._onOutage = nil
  return instance
end

--- Register callbacks.
--- @param onFlush fun(points: string[]) Called when buffer should be drained (attempt a write).
--- @param onStateChange fun(state: string) Called when connection state changes.
--- @param onOutage fun() Called when outage exceeds configured threshold.
function OfflineBuffer:setCallbacks(onFlush, onStateChange, onOutage)
  self._onFlush = onFlush
  self._onStateChange = onStateChange
  self._onOutage = onOutage
end

--- Update configuration (e.g., after property change).
--- @param opts table Table with optional fields: max_points, max_bytes, outage_threshold
function OfflineBuffer:configure(opts)
  if opts.max_points then self._config.max_points = opts.max_points end
  if opts.max_bytes then self._config.max_bytes = opts.max_bytes end
  if opts.outage_threshold then self._config.outage_threshold = opts.outage_threshold end
end

---------------------------------------------------------------------------
-- Internal helpers
---------------------------------------------------------------------------

--- Load the buffer from persistent storage.
--- @return string[] points Array of line-protocol strings.
function OfflineBuffer:_load()
  local buf = persist:get(PERSIST_KEYS.BUFFER, {})
  if type(buf) ~= "table" then
    return {}
  end
  return buf
end

--- Save the buffer to persistent storage.
--- @param buf string[] Array of line-protocol strings.
function OfflineBuffer:_save(buf)
  persist:set(PERSIST_KEYS.BUFFER, buf)
end

--- Count approximate byte size of a string array.
--- @param buf string[]
--- @return number bytes
local function byteCount(buf)
  local total = 0
  for _, s in ipairs(buf) do
    total = total + #s + 1 -- +1 for newline separator
  end
  return total
end

--- Transition to a new connection state, firing callbacks as needed.
--- @param newState string One of ConnectionState.*
function OfflineBuffer:_setState(newState)
  if self._state == newState then return end
  local prev = self._state
  self._state = newState
  log:info("Connection state: %s -> %s", prev, newState)

  if newState == ConnectionState.DISCONNECTED or newState == ConnectionState.RECONNECTING then
    if self._disconnectedAt == nil then
      self._disconnectedAt = os.time()
      self._outageNotified = false
    end
  elseif newState == ConnectionState.CONNECTED then
    self._disconnectedAt = nil
    self._outageNotified = false
    self._backoffIndex = 1
  end

  if self._onStateChange then
    self._onStateChange(newState)
  end
end

--- Get the current backoff delay in seconds.
--- @return number seconds
function OfflineBuffer:_backoffDelay()
  local idx = math.min(self._backoffIndex, #BACKOFF_SCHEDULE)
  return BACKOFF_SCHEDULE[idx]
end

--- Advance the backoff index (on failure).
function OfflineBuffer:_advanceBackoff()
  if self._backoffIndex < #BACKOFF_SCHEDULE then
    self._backoffIndex = self._backoffIndex + 1
  end
end

--- Cancel any active retry timer.
function OfflineBuffer:_cancelRetryTimer()
  if self._retryTimerId then
    C4:KillTimer(self._retryTimerId)
    self._retryTimerId = nil
  end
end

--- Schedule a retry attempt after the current backoff delay.
function OfflineBuffer:_scheduleRetry()
  self:_cancelRetryTimer()
  local delaySec = self:_backoffDelay()
  log:info("Scheduling reconnect retry in %ds (backoff index %d)", delaySec, self._backoffIndex)
  -- C4:SetTimer returns a timer ID; callback fires once then stops (interval=false)
  self._retryTimerId = C4:SetTimer(delaySec * 1000, function()
    self._retryTimerId = nil
    self:_attemptDrain()
  end, false)
end

--- Check whether the outage threshold has been exceeded and notify if so.
function OfflineBuffer:_checkOutageThreshold()
  if self._outageNotified then return end
  if self._disconnectedAt == nil then return end
  local elapsed = os.time() - self._disconnectedAt
  if elapsed >= self._config.outage_threshold then
    self._outageNotified = true
    log:warn("Extended outage detected (%ds >= threshold %ds)", elapsed, self._config.outage_threshold)
    if self._onOutage then
      self._onOutage()
    end
  end
end

--- Attempt to drain the offline buffer. Called by the retry timer.
function OfflineBuffer:_attemptDrain()
  local buf = self:_load()
  if #buf == 0 then
    log:debug("Reconnect attempt: offline buffer is empty, marking connected")
    self:_setState(ConnectionState.CONNECTED)
    return
  end

  self:_setState(ConnectionState.RECONNECTING)
  self:_checkOutageThreshold()

  log:info("Attempting to drain offline buffer (%d points)", #buf)
  if self._onFlush then
    self._onFlush(buf)
  end
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Returns the current connection state string.
--- @return string state
function OfflineBuffer:getState()
  return self._state
end

--- Returns the number of points currently in the offline buffer.
--- @return number count
function OfflineBuffer:size()
  return #self:_load()
end

--- Push a set of points into the offline buffer.
--- Applies FIFO eviction when capacity is exceeded.
--- Fires the Buffer Full event (via onOutage pathway) when eviction occurs.
--- @param points string[] Line-protocol strings to buffer.
function OfflineBuffer:push(points)
  if not points or #points == 0 then return end

  local buf = self:_load()
  for _, pt in ipairs(points) do
    buf[#buf + 1] = pt
  end

  -- Enforce limits (FIFO eviction)
  local evicted = 0
  while #buf > self._config.max_points or byteCount(buf) > self._config.max_bytes do
    table.remove(buf, 1)
    evicted = evicted + 1
  end

  if evicted > 0 then
    log:warn("Offline buffer full: evicted %d oldest point(s)", evicted)
    -- Caller should fire 'Buffer Full' event
  end

  self:_save(buf)
  log:debug("Offline buffer: %d point(s) stored (%d new, %d evicted)", #buf, #points, evicted)
  return evicted
end

--- Called when a write attempt succeeds.
--- Removes delivered points from the buffer, transitions to Connected.
--- @param deliveredCount number Number of points that were successfully written.
function OfflineBuffer:onWriteSuccess(deliveredCount)
  local buf = self:_load()

  if deliveredCount > 0 and #buf > 0 then
    -- Remove the delivered points from the front of the buffer
    local remaining = {}
    for i = deliveredCount + 1, #buf do
      remaining[#remaining + 1] = buf[i]
    end
    self:_save(remaining)
    buf = remaining
  end

  self._backoffIndex = 1

  if #buf > 0 then
    -- Still more points to drain; schedule next drain immediately (small delay to avoid spinning)
    log:info("Buffer drain in progress: %d point(s) remaining", #buf)
    self._retryTimerId = C4:SetTimer(500, function()
      self._retryTimerId = nil
      self:_attemptDrain()
    end, false)
  else
    log:info("Offline buffer drained successfully")
    self:_setState(ConnectionState.CONNECTED)
  end
end

--- Called when a write attempt fails.
--- @param isRetriable boolean Whether this error type should be retried.
--- @param points string[]|nil Points to add to the offline buffer (nil if already in buffer).
--- @return number evicted Number of points evicted due to buffer overflow (0 if none).
function OfflineBuffer:onWriteFailure(isRetriable, points)
  local evicted = 0

  if isRetriable then
    if points and #points > 0 then
      evicted = self:push(points) or 0
    end
    self:_setState(ConnectionState.DISCONNECTED)
    self:_checkOutageThreshold()
    self:_advanceBackoff()
    self:_scheduleRetry()
  else
    -- Permanent error (401, 422): don't buffer, don't change connection state
    log:warn("Permanent write error — not buffering %d point(s)", points and #points or 0)
  end

  return evicted
end

--- Notify the buffer that a drain-attempt write completed (success or failure).
--- Used when draining is in progress and we get the HTTP callback.
--- @param success boolean
--- @param deliveredCount number Points that were written (only meaningful when success=true).
--- @param isRetriable boolean (only meaningful when success=false)
function OfflineBuffer:onDrainResult(success, deliveredCount, isRetriable)
  if success then
    self:onWriteSuccess(deliveredCount)
  else
    if isRetriable then
      self:_setState(ConnectionState.DISCONNECTED)
      self:_checkOutageThreshold()
      self:_advanceBackoff()
      self:_scheduleRetry()
    else
      -- Permanent error during drain (e.g., token revoked): stop draining
      log:error("Permanent error during drain — halting retry")
      self:_cancelRetryTimer()
      self:_setState(ConnectionState.DISCONNECTED)
    end
  end
end

--- Trigger an immediate drain attempt (e.g., after user-initiated reconnect).
function OfflineBuffer:triggerDrain()
  self:_cancelRetryTimer()
  self:_attemptDrain()
end

--- Clear the offline buffer (e.g., on driver reset).
function OfflineBuffer:clear()
  self:_save({})
  log:info("Offline buffer cleared")
end

--- Destroy: cancel timers and release resources.
function OfflineBuffer:destroy()
  self:_cancelRetryTimer()
end

--- Return the ConnectionState enum for external use.
OfflineBuffer.State = ConnectionState

return OfflineBuffer
