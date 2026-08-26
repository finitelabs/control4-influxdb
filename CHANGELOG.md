# <span style="color:#109EFF">Changelog</span>

<!-- prettier-ignore-start -->
[//]: # "## v[Version] - YYY-MM-DD"
[//]: # "### Added"
[//]: # "- Added"
[//]: # "### Fixed"
[//]: # "- Fixed"
[//]: # "### Changed"
[//]: # "- Changed"
[//]: # "### Removed"
[//]: # "- Removed"
<!-- prettier-ignore-end -->

## Unreleased (BIG AV fork)

Community fork adding one click, convention based configuration and a service
intelligence layer on top of the upstream InfluxDB Data Logger.

### Added

- **Auto Configure Measurements** action. One click discovers every device
  (`C4:GetDevices` + `C4:GetDeviceVariables`), resolves each variable name to its
  numeric variable id, and creates `tv_usage`, `light_usage`, `security_status`,
  and `device_faults` measurements by convention. No manual per device binding.
- **Site** property. A literal tag written on every measurement (the home id) so a
  single database cleanly separates multiple homes.
- **device_faults** measurement. An orthogonal fault scan maps known Control4
  fault variables (`OVER_TEMPERATURE`, `SHORT_CIRCUIT_DETECTED`, `TROUBLE_TYPE`,
  `LAST_ARM_FAILED`, `UPS_POWER_LOST_BOOL`, etc.) to a `fault_active` model for
  proactive service alerting. Static severity and text live in the catalog, not
  the time series.

### Changed

- Auto Configure sets the high volume usage measurements (`tv_usage`,
  `light_usage`) to **Dedup ON** (write on change) so busy homes do not
  accumulate excess Parquet files on InfluxDB 3 Core, which has no compactor.
  `device_faults` stays Dedup OFF (heartbeat) so current fault state is always
  fresh for alerting. Compute usage duration from on/off transitions.

## v20260331 - 2026-03-31

### Added

- Web UI for configuring measurement schemas, per-device readings, and
  transforms

## v20260325 - 2026-03-25

### Added

- Initial Release
