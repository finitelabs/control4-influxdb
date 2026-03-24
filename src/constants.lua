--- Constants used throughout the InfluxDB Data Logger driver.

return {
  --- Constant for showing a property in the UI.
  --- @type number
  SHOW_PROPERTY = 0,

  --- Constant for hiding a property in the UI.
  --- @type number
  HIDE_PROPERTY = 1,

  --- Log level constants.
  --- @type table<string, string>
  LOG_LEVELS = {
    FATAL = "0 - Fatal",
    ERROR = "1 - Error",
    WARNING = "2 - Warning",
    INFO = "3 - Info",
    DEBUG = "4 - Debug",
    TRACE = "5 - Trace",
    ULTRA = "6 - Ultra",
  },

  --- Log mode constants.
  --- @type table<string, string>
  LOG_MODES = {
    OFF = "Off",
    PRINT = "Print",
    LOG = "Log",
    PRINT_AND_LOG = "Print and Log",
  },

  --- Default write interval in seconds.
  --- @type number
  DEFAULT_WRITE_INTERVAL = 60,

  --- Default write precision.
  --- @type string
  DEFAULT_PRECISION = "ms",

  --- Maximum number of points in a single batch write.
  --- @type number
  MAX_BATCH_SIZE = 5000,

  --- Maximum number of points to buffer when offline.
  --- @type number
  MAX_BUFFER_SIZE = 10000,

  --- Maximum offline buffer size in bytes (~1 MB).
  --- @type number
  MAX_BUFFER_BYTES = 1048576,

  --- Exponential backoff schedule for reconnect retries (seconds).
  --- Sequence: 5s, 15s, 30s, 1m, 5m, 15m.
  --- @type number[]
  RETRY_INTERVALS = { 5, 15, 30, 60, 300, 900 },

  --- Default outage notification threshold in seconds (5 minutes).
  --- @type number
  DEFAULT_OUTAGE_THRESHOLD = 300,

  --- Outage threshold options (display value -> seconds).
  --- @type table<string, number>
  OUTAGE_THRESHOLDS = {
    ["1m"]  = 60,
    ["5m"]  = 300,
    ["15m"] = 900,
    ["30m"] = 1800,
    ["1h"]  = 3600,
  },

  --- InfluxDB line protocol value type identifiers.
  --- @type table<string, string>
  VALUE_TYPES = {
    INTEGER = "integer",
    FLOAT = "float",
    STRING = "string",
    BOOLEAN = "boolean",
  },

  --- Write interval options (display value -> seconds).
  --- @type table<string, number>
  WRITE_INTERVALS = {
    ["10s"] = 10,
    ["30s"] = 30,
    ["1m"] = 60,
    ["5m"] = 300,
    ["15m"] = 900,
  },

  --- Write precision options.
  --- @type table<string, string>
  PRECISIONS = {
    ["ns"] = "ns",
    ["us"] = "us",
    ["ms"] = "ms",
    ["s"] = "s",
  },
}
