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

### Fixed

- Fixed the Offline Buffer and Retry heading in Composer, which read Offline
  Buffer Retry.

### Changed

- Changed the Settings tab to show the driver's properties under its status, in
  place of a separate Settings view, with each change saved at once.
- Changed the Status view to show Driver Status as Composer does, with the time
  of the last write and how many measurements and readings are set up.
  <!-- #ifdef DRIVERCENTRAL -->
- Changed Log Level to stop at 4 - Debug unless a Support Code from Finite Labs
  support has the Lua tab open.
  <!-- #endif -->

## v20261107 - 2026-11-07

<!-- #ifdef DRIVERCENTRAL -->

### Added

- Added a Support Code property to the InfluxDB Data Logger driver, which opens
  the driver's Lua tab in Composer for an hour with a code from Finite Labs
  support.
- Added a Cloud Monitoring property, Enabled by default, that sends Finite Labs
  an hourly report of the driver's memory use and a report of each error it
  hits, and a Diagnostics Snapshot action.

<!-- #endif -->

### Fixed

- Fixed the driver holding far too much unsent data during an InfluxDB outage,
  so it now keeps at most 8 MB and drops the oldest points first.
- Fixed a failing write being sent again every 5 seconds while InfluxDB was
  down, so retries now wait longer each time, up to 15 minutes.
- Fixed Driver Status reading Connected, and the Disconnected event not firing,
  while the server could not be reached.
- Fixed the Extended Outage event never firing, so it now fires once per outage
  after the Outage Notification Threshold.
- Fixed the Buffer Full event firing for every point lost, so it now fires once
  each time points start being lost.
- Fixed writes for a measurement stopping until the driver was reloaded when a
  request to InfluxDB never got an answer.
- Fixed a logged variable whose value is not a finite number being written as a
  wrong number, or making InfluxDB refuse the rest of the write.
- Fixed every point being written at the wrong time when Write Precision was set
  to anything but ms.
- Fixed saving text in the Settings tab that held accented letters, other
  alphabets or a less-than sign.
  <!-- #ifndef DRIVERCENTRAL -->
- Fixed changing Automatic Updates or Update Channel on one InfluxDB driver
  briefly holding up the others.
  <!-- #endif -->

<!-- #ifdef DRIVERCENTRAL -->

### Changed

- Changed Automatic Updates to one setting shared by every InfluxDB driver in
  the project.

<!-- #endif -->

### Removed

- Removed the Max Buffer Size and Offline Buffer Size properties and the Clear
  Offline Buffer action, which never did anything, since the driver now limits
  unsent data itself.

## v20260816 - 2026-08-16

### Added

- A schema field can pin its InfluxDB type instead of inferring it from each
  value, so a measurement whose readings mix whole and fractional numbers is no
  longer rejected, and the mapping preview shows the pinned type.
- Variables on agents, including Composer global variables, can be logged.

### Fixed

- All of a measurement's readings on the same schedule are sent in one write,
  instead of one request each.
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
- A device now appears once in the device picker instead of once for each proxy
  of the same name.
- Dropdowns and the Delete button in the measurement editor are no longer cut
  off or pushed out of reach.

### Changed

- The web UI fills the available width and adapts to narrow panes.

## v20260331 - 2026-03-31

### Added

- Web UI for configuring measurement schemas, per-device readings, and
  transforms

## v20260325 - 2026-03-25

### Added

- Initial Release
