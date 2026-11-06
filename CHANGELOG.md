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

## v20261106 - 2026-11-06

<!-- #ifdef DRIVERCENTRAL -->

### Added

- Added a `Support Code` property to the InfluxDB Data Logger driver. A code
  from Finite Labs support opens the driver's Lua tab in Composer for an hour;
  leave it blank otherwise.
- Added a `Cloud Monitoring` property, Enabled by default, and a
  `Diagnostics Snapshot` action to the driver. Cloud Monitoring sends Finite
  Labs an hourly report of the driver's memory use and a report of each error it
  hits; the documentation says what they hold.

<!-- #endif -->

### Fixed

- Fixed the driver holding far more unsent data in memory than it should during
  an InfluxDB outage. It now holds at most 8 MB across all its measurements and
  drops the oldest points from the largest first. Once delivery resumes, each
  reading writes its current value again.
- Fixed a failing write being sent again every 5 seconds for as long as InfluxDB
  was down or overloaded. Retries now wait longer each time, up to 15 minutes,
  and start over once a write succeeds, so points can reach InfluxDB up to 15
  minutes after it's back.
- Fixed `Driver Status` reading `Connected`, and the `Disconnected` event not
  firing, while writes failed because the server couldn't be reached.
- Fixed the `Extended Outage` event never firing. It now fires once writes to
  InfluxDB have failed for the `Outage Notification Threshold`, once per outage.
- Fixed the `Buffer Full` event firing for every point lost. It now fires once
  each time points start being lost, and again only after delivery resumes.
- Fixed writes for a measurement stopping until the driver was reloaded when a
  request to InfluxDB never got an answer. The driver now sends those points
  again after a few minutes, and the new `INFLUX_WATCHDOG_FIRES` variable counts
  each time.
- Fixed a logged variable whose value isn't a finite number being written as a
  wrong number, or making InfluxDB refuse the other points sent with it. That
  value is now left out and the rest of the reading is written.
- Fixed saving text in the Settings tab. A character outside Western European
  alphabets made the save do nothing, and an accented letter was stored wrongly.
  A reading or field named that way now keeps its own mappings, and a property
  holding `<` now saves.
  <!-- #ifndef DRIVERCENTRAL -->
- Fixed changing Automatic Updates or Update Channel on an InfluxDB driver
  briefly holding up every other InfluxDB driver, longer in a project with many,
  while the change was copied to them.
  <!-- #endif -->

<!-- #ifdef DRIVERCENTRAL -->

### Changed

- Changed Automatic Updates to one setting for every InfluxDB driver in the
  project: changing it on any of them now changes it on all of them.
- Changed the driver to stop without an active DriverCentral license or trial.
  Five minutes after the license lapses it collects, writes and tests nothing,
  and Driver Status reads `No active license`; buffered points wait. A license
  starts it again at once.

<!-- #endif -->

### Removed

- Removed the `Max Buffer Size` and `Offline Buffer Size` properties and the
  `Clear Offline Buffer` action, which never did anything. The driver limits
  unsent data itself, and the `INFLUX_POINTS_BUFFERED` variable shows how many
  points are waiting.

## v20260816 - 2026-08-16

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
