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
