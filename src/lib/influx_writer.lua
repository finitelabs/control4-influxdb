--- InfluxDB Write Client and Batch Engine

local log = require("lib.logging")
local constants = require("constants")
local Deferred = require("deferred")
local values = require("lib.values")

require("drivers-common-public.global.timer")

---------------------------------------------------------------------------
-- Module
---------------------------------------------------------------------------

--- @class InfluxWriter
local InfluxWriter = {}
InfluxWriter.__index = InfluxWriter

---------------------------------------------------------------------------
-- Line Protocol Helpers
---------------------------------------------------------------------------

--- Escape special characters in a measurement name.
--- Measurement names: escape commas and spaces.
--- @param s string
--- @return string
local function escapeMeasurement(s)
  s = tostring(s)
  s = s:gsub(",", "\\,")
  s = s:gsub(" ", "\\ ")
  return s
end

--- Escape special characters in a tag key, tag value, or field key.
--- These must escape: commas, equals, spaces.
--- @param s string
--- @return string
local function escapeTagOrKey(s)
  s = tostring(s)
  s = s:gsub(",", "\\,")
  s = s:gsub("=", "\\=")
  s = s:gsub(" ", "\\ ")
  return s
end

--- Escape special characters in a field string value.
--- String values are double-quoted; escape double quotes and backslashes inside.
--- @param s string
--- @return string
local function escapeFieldString(s)
  s = tostring(s)
  s = s:gsub("\\", "\\\\")
  s = s:gsub('"', '\\"')
  return '"' .. s .. '"'
end

--- Infer an InfluxDB value type from a raw value. Beside formatFieldValue so
--- the write path and the UI's preview cannot disagree.
--- @param val any
--- @return string valueType One of "integer", "float", "string", "boolean"
function InfluxWriter.inferValueType(val)
  if val == nil then
    return constants.VALUE_TYPES.STRING
  end
  local s = tostring(val)
  local low = s:lower()
  if low == "true" or low == "false" then
    return constants.VALUE_TYPES.BOOLEAN
  end
  local n = tonumber(s)
  if n then
    if math.floor(n) == n and math.abs(n) < 2 ^ 53 then
      return constants.VALUE_TYPES.INTEGER
    end
    return constants.VALUE_TYPES.FLOAT
  end
  return constants.VALUE_TYPES.STRING
end

--- Coerce and format a field value for InfluxDB line protocol.
--- @param value any
--- @param valueType string One of "integer", "float", "string", "boolean"
--- @return string|nil formatted, string|nil err
local function formatFieldValue(value, valueType)
  if valueType == constants.VALUE_TYPES.INTEGER then
    local n = tonumber(value)
    if n == nil then
      return nil, string.format("cannot coerce '%s' to integer", tostring(value))
    end
    return string.format("%di", math.floor(n))
  elseif valueType == constants.VALUE_TYPES.FLOAT then
    local n = tonumber(value)
    if n == nil then
      return nil, string.format("cannot coerce '%s' to float", tostring(value))
    end
    -- Always include decimal point to ensure float typing
    local s = string.format("%.15g", n)
    if not s:find("%.") and not s:find("e") then
      s = s .. ".0"
    end
    return s
  elseif valueType == constants.VALUE_TYPES.STRING then
    return escapeFieldString(tostring(value))
  elseif valueType == constants.VALUE_TYPES.BOOLEAN then
    local t = type(value)
    if t == "boolean" then
      return value and "true" or "false"
    elseif t == "string" then
      local lower = value:lower()
      if lower == "true" or lower == "t" or lower == "yes" or lower == "1" then
        return "true"
      elseif lower == "false" or lower == "f" or lower == "no" or lower == "0" then
        return "false"
      end
    elseif t == "number" then
      return (value ~= 0) and "true" or "false"
    end
    return nil, string.format("cannot coerce '%s' to boolean", tostring(value))
  else
    return nil, string.format("unknown value type '%s'", tostring(valueType))
  end
end

--- Exposed so the preview types a value the way the write path does.
InfluxWriter.formatFieldValue = formatFieldValue

--- Build a single InfluxDB line protocol string.
--- @param measurement string   Measurement name
--- @param tags table<string,string>   Tag key/value pairs (may be empty)
--- @param fields table<string,{value:any, type:string}>  Field definitions
--- @param timestampMs number|nil  Timestamp in milliseconds (nil = let InfluxDB assign)
--- @return string|nil line, string|nil err
function InfluxWriter.buildLine(measurement, tags, fields, timestampMs)
  if not measurement or measurement == "" then
    return nil, "measurement name is required"
  end

  -- Measurement
  local line = escapeMeasurement(measurement)

  -- Tags (sorted for consistency and compression)
  local tagKeys = {}
  for k in pairs(tags or {}) do
    tagKeys[#tagKeys + 1] = k
  end
  table.sort(tagKeys)

  for _, k in ipairs(tagKeys) do
    local v = tags[k]
    if v ~= nil and tostring(v) ~= "" then
      line = line .. "," .. escapeTagOrKey(k) .. "=" .. escapeTagOrKey(tostring(v))
    end
  end

  -- Fields (at least one required)
  local fieldParts = {}
  for fieldKey, fieldDef in pairs(fields or {}) do
    local formatted, err = formatFieldValue(fieldDef.value, fieldDef.type)
    if formatted then
      fieldParts[#fieldParts + 1] = escapeTagOrKey(fieldKey) .. "=" .. formatted
    else
      log:warn("buildLine: skipping field '%s': %s", fieldKey, err or "unknown error")
    end
  end

  if #fieldParts == 0 then
    return nil, "at least one valid field is required"
  end

  table.sort(fieldParts) -- deterministic ordering
  line = line .. " " .. table.concat(fieldParts, ",")

  -- Optional timestamp
  if timestampMs then
    line = line .. " " .. string.format("%d", timestampMs)
  end

  return line
end

---------------------------------------------------------------------------
-- HTTP Write Client
---------------------------------------------------------------------------

--- Error classification constants.
--- @type table<string, boolean>
local RETRIABLE_CODES = {
  [429] = true,
  [500] = true,
  [502] = true,
  [503] = true,
  [504] = true,
}

--- @param responseCode number
--- @return boolean isRetriable
local function isRetriable(responseCode)
  return RETRIABLE_CODES[responseCode] == true
end

--- Post a batch of line-protocol strings to InfluxDB.
--- Returns a Deferred that resolves with { count = number } on success,
--- or rejects with { retriable = bool, retryAfter = number|nil, errMsg = string } on failure.
--- @param url string        Full write endpoint URL
--- @param token string      API token (may be empty string)
--- @param lines string[]    Array of line-protocol strings
--- @return Deferred
function InfluxWriter.postBatch(url, token, lines)
  local d = Deferred.new()

  if not lines or #lines == 0 then
    d:resolve({ count = 0 })
    return d
  end

  local payload = table.concat(lines, "\n")

  local headers = {
    ["Content-Type"] = "text/plain; charset=utf-8",
  }
  if token and token ~= "" then
    headers["Authorization"] = "Token " .. token
  end

  log:debug("InfluxWriter.postBatch: posting %d lines to %s", #lines, url)

  C4:urlPost(url, payload, headers, false, function(ticketId, strData, responseCode, tHeaders, strError)
    -- Network-level error
    if strError and strError ~= "" then
      log:error("InfluxWriter: network error: %s", strError)
      d:reject({ retriable = true, retryAfter = nil, errMsg = "network error: " .. strError })
      return
    end

    if responseCode == 200 or responseCode == 204 then
      log:debug("InfluxWriter: write OK (HTTP %d), %d points", responseCode, #lines)
      d:resolve({ count = #lines })
    elseif responseCode == 401 then
      log:error("InfluxWriter: authentication failed (HTTP 401) — check API token")
      d:reject({ retriable = false, retryAfter = nil, errMsg = "authentication error (HTTP 401)" })
    elseif responseCode == 422 then
      log:error("InfluxWriter: parse error (HTTP 422): %s", strData or "")
      d:reject({
        retriable = false,
        retryAfter = nil,
        errMsg = "line protocol parse error (HTTP 422): " .. (strData or ""),
      })
    elseif responseCode == 429 then
      -- Parse Retry-After header if present
      local retryAfter = nil
      if tHeaders then
        local ra = tHeaders["Retry-After"] or tHeaders["retry-after"]
        if ra then
          retryAfter = tonumber(ra)
        end
      end
      log:warn("InfluxWriter: rate limited (HTTP 429), retry-after=%s", tostring(retryAfter))
      d:reject({ retriable = true, retryAfter = retryAfter, errMsg = "rate limited (HTTP 429)" })
    elseif responseCode >= 500 then
      log:error("InfluxWriter: server error (HTTP %d): %s", responseCode, strData or "")
      d:reject({
        retriable = isRetriable(responseCode),
        retryAfter = nil,
        errMsg = string.format("server error (HTTP %d)", responseCode),
      })
    else
      log:error("InfluxWriter: unexpected response (HTTP %d): %s", responseCode, strData or "")
      d:reject({ retriable = false, retryAfter = nil, errMsg = string.format("unexpected HTTP %d", responseCode) })
    end
  end)

  return d
end

---------------------------------------------------------------------------
-- Batch Engine
---------------------------------------------------------------------------

--- Create a new InfluxWriter instance (batch engine).
---
--- @param opts table Configuration options:
---   - getConfig: function() -> {url, token, database, precision}  (required)
---   - onConnected: function(bool)         called on successful/failed write
---   - onWriteError: function(errMsg)      called on write error
---   - onBufferFull: function()            called when buffer evicts oldest points
--- @return InfluxWriter
function InfluxWriter:new(opts)
  opts = opts or {}
  local instance = setmetatable({}, self)

  --- Callback: function() -> {url, token, database, precision}
  instance._getConfig = opts.getConfig or function()
    return {}
  end
  instance._onConnected = opts.onConnected
  instance._onWriteError = opts.onWriteError
  instance._onBufferFull = opts.onBufferFull

  --- Per-buffer state keyed by measurement and interval (see stateKeyFor), so
  --- one flush carries every reading sharing a measurement and a schedule.
  --- @type table<string, table>
  instance._measurements = {}

  --- Global metrics
  instance._metrics = {
    pointsBuffered = 0,
    pointsWritten = 0,
    pointsDropped = 0,
    writeErrors = 0,
    lastWriteTimestamp = 0,
  }

  return instance
end

--- Buffer key for a measurement written on a given schedule.
---
--- Not keyed per reading: that made a tick issue one HTTP request per reading,
--- which saturated the controller's HTTP client and timed the writes out.
--- @param measurementName string
--- @param intervalSecs number|nil
--- @return string stateKey
local function stateKeyFor(measurementName, intervalSecs)
  return measurementName .. "@" .. tostring(intervalSecs or constants.DEFAULT_WRITE_INTERVAL)
end

--- Get or create per-buffer state.
--- @param stateKey string From stateKeyFor().
--- @param measurementName string The name the buffer belongs to.
--- @param intervalSecs number
--- @param maxBuffer number
--- @param dedupEnabled boolean
--- @return table state
function InfluxWriter:_getMeasurementState(stateKey, measurementName, intervalSecs, maxBuffer, dedupEnabled)
  if not self._measurements[stateKey] then
    self._measurements[stateKey] = {
      -- Kept so buffers can be found by name. Matching on a stateKey prefix
      -- instead would let a measurement called "power" claim "power@rack".
      measurementName = measurementName,
      buffer = {},
      timerName = nil,
      -- dedup scope -> field key -> last buffered value. Scoped per reading:
      -- readings share a buffer and would otherwise dedup against each other.
      lastValues = {},
      lastFlushTime = 0,
      inFlight = false,
      intervalSecs = intervalSecs or constants.DEFAULT_WRITE_INTERVAL,
      maxBuffer = maxBuffer or constants.MAX_BUFFER_SIZE,
      dedupEnabled = dedupEnabled ~= false, -- default true
    }
  end
  return self._measurements[stateKey]
end

--- Update driver variables with current metrics via the values lib.
function InfluxWriter:_updateMetricVariables()
  local m = self._metrics
  values:update("INFLUX_POINTS_BUFFERED", m.pointsBuffered, "INT")
  values:update("INFLUX_POINTS_WRITTEN", m.pointsWritten, "INT")
  values:update("INFLUX_POINTS_DROPPED", m.pointsDropped, "INT")
  values:update("INFLUX_WRITE_ERRORS", m.writeErrors, "INT")
  values:update("INFLUX_LAST_WRITE_TS", m.lastWriteTimestamp > 0 and m.lastWriteTimestamp or "", "STRING")
end

--- Enqueue a data point for a measurement. Handles dedup and FIFO eviction.
---
--- @param measurementName string
--- @param tags table<string,string>
--- @param fields table<string,{value:any, type:string}>
--- @param opts table  { interval:number, maxBuffer:number, dedup:boolean, dedupKey:string|nil }
--- @param timestampMs number|nil
function InfluxWriter:enqueue(measurementName, tags, fields, opts, timestampMs)
  opts = opts or {}
  local stateKey = stateKeyFor(measurementName, opts.interval)
  local dedupScope = opts.dedupKey or measurementName
  local state = self:_getMeasurementState(stateKey, measurementName, opts.interval, opts.maxBuffer, opts.dedup)

  -- Build the line first (so we can check dedup before buffering)
  local line, err = InfluxWriter.buildLine(measurementName, tags, fields, timestampMs)
  if not line then
    log:warn("InfluxWriter.enqueue: skipping point for '%s': %s", measurementName, err or "")
    return
  end

  -- Dedup check: skip if all field values unchanged since last flush
  -- Per-call opts.dedup can override the measurement's default dedup setting
  local dedupActive = state.dedupEnabled
  if opts.dedup ~= nil then
    dedupActive = opts.dedup
  end
  if dedupActive then
    local lastValues = state.lastValues[dedupScope]
    local changed = lastValues == nil
    if not changed then
      for fieldKey, fieldDef in pairs(fields) do
        if lastValues[fieldKey] ~= tostring(fieldDef.value) then
          changed = true
          break
        end
      end
    end
    if not changed then
      log:trace("InfluxWriter.enqueue: dedup skip for '%s' (no value change)", dedupScope)
      return
    end
  end

  -- FIFO eviction if at max capacity
  if #state.buffer >= state.maxBuffer then
    table.remove(state.buffer, 1)
    self._metrics.pointsDropped = self._metrics.pointsDropped + 1
    self._metrics.pointsBuffered = math.max(0, self._metrics.pointsBuffered - 1)
    log:warn("InfluxWriter: buffer full for '%s', evicting oldest point", measurementName)
    if self._onBufferFull then
      pcall(self._onBufferFull)
    end
  end

  state.buffer[#state.buffer + 1] = line
  self._metrics.pointsBuffered = self._metrics.pointsBuffered + 1
  self:_updateMetricVariables()

  -- Store last seen values for next dedup check
  local lastValues = state.lastValues[dedupScope] or {}
  for fieldKey, fieldDef in pairs(fields) do
    lastValues[fieldKey] = tostring(fieldDef.value)
  end
  state.lastValues[dedupScope] = lastValues

  log:trace("InfluxWriter.enqueue: buffered point for '%s' (%d in buffer)", stateKey, #state.buffer)
end

--- Arm (or re-arm) the flush timer for a measurement.
--- @param stateKey string
--- @param state table
function InfluxWriter:_armFlushTimer(stateKey, state)
  local timerName = "InfluxWriter_" .. stateKey
  if state.timerName then
    return -- already armed
  end

  local intervalMs = (state.intervalSecs or constants.DEFAULT_WRITE_INTERVAL) * 1000
  log:trace("InfluxWriter: arming flush timer for '%s' (%ds)", stateKey, state.intervalSecs)

  state.timerName = timerName
  SetTimer(timerName, intervalMs, function()
    state.timerName = nil
    self:_flushMeasurement(stateKey)
  end)
end

--- Flush a single measurement's buffer to InfluxDB.
---
--- Never more than one request outstanding per buffer: overlapping flushes fed
--- the load that caused their own timeouts. Callers wanting an immediate flush
--- cancel the armed timer first; there is no flag to bypass this.
--- @param stateKey string
function InfluxWriter:_flushMeasurement(stateKey)
  local state = self._measurements[stateKey]
  if not state or #state.buffer == 0 then
    return
  end

  if state.inFlight then
    log:debug("InfluxWriter: flush for '%s' already in flight, re-arming", stateKey)
    self:_armFlushTimer(stateKey, state)
    return
  end

  -- Build URL from current config
  local cfg = self._getConfig()
  if not cfg or not cfg.url or cfg.url == "" or not cfg.database or cfg.database == "" then
    log:warn("InfluxWriter: cannot flush '%s' — InfluxDB not configured", stateKey)
    -- Re-arm so we retry later
    self:_armFlushTimer(stateKey, state)
    return
  end

  local base = cfg.url:gsub("/$", "")
  local url = string.format(
    "%s/api/v2/write?bucket=%s&precision=%s",
    base,
    cfg.database,
    cfg.precision or constants.DEFAULT_PRECISION
  )

  -- Take a snapshot of the buffer (up to MAX_BATCH_SIZE)
  local batchSize = math.min(#state.buffer, constants.MAX_BATCH_SIZE)
  local batch = {}
  for i = 1, batchSize do
    batch[i] = state.buffer[i]
  end

  -- Remove flushed entries
  local remaining = {}
  for i = batchSize + 1, #state.buffer do
    remaining[#remaining + 1] = state.buffer[i]
  end
  state.buffer = remaining
  self._metrics.pointsBuffered = math.max(0, self._metrics.pointsBuffered - batchSize)

  state.lastFlushTime = os.time()
  state.inFlight = true

  log:info("InfluxWriter: flushing %d points for '%s' (%d remaining)", batchSize, stateKey, #state.buffer)

  InfluxWriter.postBatch(url, cfg.token or "", batch)
    :next(function(result)
      state.inFlight = false
      self._metrics.pointsWritten = self._metrics.pointsWritten + batchSize
      self._metrics.lastWriteTimestamp = os.time()
      self:_updateMetricVariables()

      if self._onConnected then
        pcall(self._onConnected, true)
      end

      -- Left by a MAX_BATCH_SIZE cap or enqueued mid-request.
      if #state.buffer > 0 then
        self:_armFlushTimer(stateKey, state)
      end
    end)
    :next(nil, function(err)
      state.inFlight = false
      self._metrics.writeErrors = self._metrics.writeErrors + 1
      self:_updateMetricVariables()

      if self._onWriteError then
        pcall(self._onWriteError, err.errMsg)
      end

      if err.retriable then
        -- Put batch back at the front of the buffer
        local restored = {}
        for _, l in ipairs(batch) do
          restored[#restored + 1] = l
        end
        for _, l in ipairs(state.buffer) do
          restored[#restored + 1] = l
        end
        state.buffer = restored
        self._metrics.pointsBuffered = self._metrics.pointsBuffered + batchSize

        -- Schedule retry
        local delaySecs = err.retryAfter or constants.RETRY_INTERVALS[1]
        log:info("InfluxWriter: scheduling retry for '%s' in %ds", stateKey, delaySecs)
        local timerName = "InfluxWriter_" .. stateKey
        state.timerName = timerName
        SetTimer(timerName, delaySecs * 1000, function()
          state.timerName = nil
          self:_flushMeasurement(stateKey)
        end)
      else
        -- Permanent error — drop the batch, log it
        log:error(
          "InfluxWriter: dropping %d points for '%s' (permanent error: %s)",
          batchSize,
          stateKey,
          err.errMsg or "unknown"
        )
        self._metrics.pointsDropped = self._metrics.pointsDropped + batchSize
        self:_updateMetricVariables()

        if self._onConnected then
          pcall(self._onConnected, false)
        end
      end
    end)
end

--- Force-flush all measurement buffers immediately.
function InfluxWriter:forceFlushAll()
  log:info("InfluxWriter: force-flushing all measurements")
  for name, state in pairs(self._measurements) do
    -- Cancel existing timer so we don't double-flush
    if state.timerName then
      CancelTimer(state.timerName)
      state.timerName = nil
    end
    if #state.buffer > 0 then
      self:_flushMeasurement(name)
    end
  end
end

--- Force-flush a measurement's buffers immediately, across every interval it
--- has readings on.
--- @param measurementName string
function InfluxWriter:forceFlush(measurementName)
  for stateKey, state in pairs(self._measurements) do
    if state.measurementName == measurementName then
      if state.timerName then
        CancelTimer(state.timerName)
        state.timerName = nil
      end
      self:_flushMeasurement(stateKey)
    end
  end
end

--- Stop all flush timers and flush all buffers (call on driver shutdown).
--- Note: HTTP callbacks may still fire asynchronously after this.
function InfluxWriter:shutdown()
  log:info("InfluxWriter: shutting down, flushing all buffers")

  for name, state in pairs(self._measurements) do
    if state.timerName then
      CancelTimer(state.timerName)
      state.timerName = nil
    end
  end

  -- Best-effort flush of all buffers
  self:forceFlushAll()
end

--- Remove a measurement from the batch engine (cancel timers, discard buffers).
--- Drops every buffer: readings on different intervals hold one each.
--- @param measurementName string
function InfluxWriter:removeMeasurement(measurementName)
  for stateKey, state in pairs(self._measurements) do
    if state.measurementName == measurementName then
      if state.timerName then
        CancelTimer(state.timerName)
      end

      local discarded = #state.buffer
      if discarded > 0 then
        log:warn("InfluxWriter: discarding %d buffered points for removed measurement '%s'", discarded, stateKey)
        self._metrics.pointsBuffered = math.max(0, self._metrics.pointsBuffered - discarded)
        self._metrics.pointsDropped = self._metrics.pointsDropped + discarded
        self:_updateMetricVariables()
      end

      self._measurements[stateKey] = nil
    end
  end
end

--- Forget a reading's dedup history, so a reading re-added with the same label
--- is not deduped against the values the old one last wrote.
--- @param measurementName string
--- @param dedupScope string The reading's dedup key, as passed to enqueue().
function InfluxWriter:removeReading(measurementName, dedupScope)
  for _, state in pairs(self._measurements) do
    if state.measurementName == measurementName then
      state.lastValues[dedupScope] = nil
    end
  end
end

--- Return current metrics snapshot.
--- @return table metrics
function InfluxWriter:getMetrics()
  local m = self._metrics
  return {
    pointsBuffered = m.pointsBuffered,
    pointsWritten = m.pointsWritten,
    pointsDropped = m.pointsDropped,
    writeErrors = m.writeErrors,
    lastWriteTimestamp = m.lastWriteTimestamp,
  }
end

---------------------------------------------------------------------------
-- Module exports
---------------------------------------------------------------------------

return InfluxWriter
