-- A DriverCentral build made before its product exists (dc_pid 0) stops itself at its load: the cloud client stays
-- out, and the logger collects, writes and tests nothing, whatever it is configured with.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_license_no_product.lua

require("c4_shim")
local T = require("testlib")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

local posts = common.boot({ AC = 1, AS = "License Activated" }, 0)

T.section("With no product, the logger is stopped from its load")
do
  T.eq("the cloud client is not loaded", package.loaded["cloud-client-byte"], nil)
  T.eq("Driver Status", Properties["Driver Status"], "No active license")
  OnWatchedVariableChanged(common.SENSOR, 1000, "101")
  for _ = 1, 40 do
    ShimFireTimers()
  end
  Properties["InfluxDB URL"] = "http://198.51.100.20:8181"
  OnPropertyChanged("InfluxDB URL")
  T.eq("nothing goes out: no connection test, write or retry", posts, {})
end

T.finish()
