# <span style="color:#020A47">Changelog</span>

<!--
Template for a new release entry (copy below the heading, fill in, uncomment):

## v[Version] - YYYY-MM-DD

### Added
- Added

### Fixed
- Fixed

### Changed
- Changed

### Removed
- Removed
-->

## Unreleased

### Added

- A schema field can pin its InfluxDB type instead of inferring it from each
  value, so a measurement whose readings mix whole and fractional numbers is no
  longer rejected. The mapping preview shows the pinned type, so `0`, `0.0`,
  `"0"` and `false` are told apart and a lossy pin is visible before saving.
- Agents are listed in the device picker under their own heading, so variables
  that live on an agent, such as Composer global variables, can be logged.

### Fixed

- All of a measurement's readings on the same schedule are sent in one write,
  instead of one request per reading saturating the controller until writes
  timed out.
- Points carry the time they were read, rather than the time the write arrived,
  which drifted by minutes whenever a write was retried.
- A measurement no longer starts a write while one is still outstanding, so a
  slow or unreachable InfluxDB no longer builds up retries it cannot clear.
- Reloading or removing the driver while InfluxDB is unreachable stops cleanly
  instead of erroring during shutdown.
- A variable that never changes after startup is logged instead of being skipped
  indefinitely.
- Disabling a measurement stops its writes instead of leaving its buffered
  points retrying.
- A value of zero shows in the mapping preview instead of appearing blank.
- A device's own variables are always listed before its proxies' in the variable
  picker.
- Device and variable dropdowns in the measurement editor are no longer cut off
  by the surrounding card, and stay on screen near the edges of the pane.
- A device that a driver exposes through a proxy of the same name now appears
  once in the device picker instead of several times, and its variable list is
  grouped by where each variable comes from. Proxies that stand for something of
  their own, such as a security panel's areas or a receiver's tuner, are still
  listed separately. Devices that genuinely share a room and name are told apart
  by their device id. Devices with no variables are hidden when mapping a field
  or tag, and remain available under Device Tags.
- A long measurement name no longer pushes the Delete button out of reach in the
  measurements table.

### Changed

- The web UI fills the available width and adapts to narrow panes.

## v20260331 - 2026-03-31

### Added

- Web UI for configuring measurement schemas, per-device readings, and
  transforms

## v20260325 - 2026-03-25

### Added

- Initial Release
