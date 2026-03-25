# InfluxDB Data Logger

Logs Control4 system variables to InfluxDB 3 for time-series monitoring and
visualization. Build Grafana dashboards for HVAC performance, lighting levels,
security state, and anything else exposed as a Control4 variable.

## System Requirements

- Control4 OS 3.3 or later
- InfluxDB 3.x instance (self-hosted or InfluxDB Cloud)
- Network access from the Control4 controller to the InfluxDB host

## Installation

1.  Download the latest `influxdb.c4z` from
    [GitHub Releases](https://github.com/finitelabs/control4-influxdb/releases).
2.  Open Composer Pro and navigate to **System Design → Drivers → Add Driver**.
3.  Browse to the downloaded `.c4z` file and add it.
4.  The driver appears under the **Utility** category.

## Configuration

### InfluxDB Connection

Set these properties under the **InfluxDB Connection** section before clicking
**Test Connection**.

| Property               | Description                                                                          |
| ---------------------- | ------------------------------------------------------------------------------------ |
| InfluxDB URL           | Full URL including scheme and port, e.g. `http://192.168.1.100:8086`                 |
| API Token              | Token with **write** permission on the target database (see below)                   |
| Database               | InfluxDB database name to write into                                                 |
| Write Precision        | Timestamp precision (`ms` is the default and works for all residential use cases)    |
| Default Write Interval | How often buffered points are flushed; shorter = more real-time, higher write volume |

**Getting an API Token:** In InfluxDB, go to **Load Data → API Tokens → Generate
API Token**. Select "Custom API Token", choose your database, and enable the
**Write** permission. Copy the generated token into the driver property.

**Write Interval trade-offs:**

- `10s` / `30s` — near real-time graphs; higher write throughput
- `1m` — good default balance; data is at most 1 minute stale
- `5m` / `15m` — minimal write load; coarser time resolution in graphs

After filling in the connection settings, click **Test Connection** to verify
the driver can reach the InfluxDB health endpoint. The **Driver Status** and
**Connection State** properties will reflect the result.

### Creating Measurements

A _measurement_ is an InfluxDB table. Each measurement holds one or more
variables (fields or tags) written on the same schedule.

1.  Type a name into the **Measurement Name** property field (e.g.
    `living_room_thermostat`).
2.  Click the **Add Measurement** action. The measurement now appears in the
    **Select Measurement** dropdown.
3.  Select the measurement from **Select Measurement**.
4.  Use the **Select Variable** (VARIABLE_SELECTOR) property to browse and pick
    a Control4 system variable (any device or room variable).
5.  Set **Add Variable As** to **Field** or **Tag** (see below), then click
    **Add Variable** — the variable is immediately associated with the
    measurement.
6.  Repeat steps 4–5 for every variable you want in this measurement.
7.  Check **Configured Variables** to confirm your variable list.
8.  Optionally override **Measurement Write Interval** (overrides the global
    default for this measurement only) and **Measurement Enabled**.

**Fields vs. Tags:**

| Type  | Use for                                                    | Examples                            |
| ----- | ---------------------------------------------------------- | ----------------------------------- |
| Field | Numeric or changing values you want to plot on a graph     | temperature, humidity, dimmer level |
| Tag   | Categorical metadata for filtering and grouping in Grafana | device name, room name, mode string |

Fields are stored as time-series data. Tags are indexed and are best kept
low-cardinality (a handful of distinct values, not a unique ID per point).

To remove a variable from a measurement, select it in **Remove Variable** and
confirm. To delete an entire measurement, select **Remove** in the **Remove
Measurement** dropdown while that measurement is active.

### Offline Buffer & Retry

When InfluxDB is unreachable, the driver stores points in an in-memory offline
buffer and retries with exponential backoff instead of dropping data.

- **Max Buffer Size** — maximum number of line-protocol points to hold (default:
  10,000). Once full, new points are dropped and the **Buffer Full** event
  fires.
- **Outage Notification Threshold** — after this duration offline, the
  **Extended Outage** event fires. Use this in Control4 programming to send an
  alert.
- **Offline Buffer Size** (read-only) — current point count in the buffer.

**Buffer actions:**

| Action               | Description                                                            |
| -------------------- | ---------------------------------------------------------------------- |
| Flush Buffer         | Immediately attempt to write the current in-memory write buffer        |
| Force Flush All      | Flush both the write buffer and the offline buffer                     |
| Drain Offline Buffer | Attempt to drain buffered points to InfluxDB (triggered automatically) |
| Clear Offline Buffer | Discard all buffered points without writing                            |

## Properties Reference

| Property                      | Type              | Default      | Description                                                  |
| ----------------------------- | ----------------- | ------------ | ------------------------------------------------------------ |
| InfluxDB URL                  | String            | _(empty)_    | InfluxDB server URL including port                           |
| API Token                     | String (pw)       | _(empty)_    | Write-permission token for the target database               |
| Database                      | String            | _(empty)_    | InfluxDB database / bucket name                              |
| Write Precision               | List              | `ms`         | Timestamp precision: `ns`, `us`, `ms`, `s`                   |
| Default Write Interval        | List              | `1m`         | Flush cadence: `10s`, `30s`, `1m`, `5m`, `15m`               |
| Max Buffer Size               | String            | `10000`      | Max offline buffer point count                               |
| Outage Notification Threshold | List              | `5m`         | Duration before Extended Outage event: `1m`–`1h`             |
| Offline Buffer Size           | String (RO)       | _(auto)_     | Current buffered point count                                 |
| Connection State              | String (RO)       | Disconnected | `Connected`, `Disconnected`, or `Reconnecting`               |
| Driver Status                 | String (RO)       | _(auto)_     | Human-readable status line                                   |
| Driver Version                | String (RO)       | _(auto)_     | Current driver version                                       |
| Log Level                     | List              | `3 - Info`   | Verbosity: `0 - Fatal` through `6 - Ultra`                   |
| Log Mode                      | List              | `Off`        | `Off`, `Print`, `Log`, or `Print and Log`                    |
| Automatic Updates             | List              | `On`         | Auto-update from GitHub Releases                             |
| Update Channel                | List              | `Production` | `Production` or `Prerelease`                                 |
| Measurement Name              | String            | _(empty)_    | Name for a new measurement (then click Add Measurement)      |
| Select Measurement            | Dynamic List      | _(auto)_     | Pick a measurement to configure                              |
| Select Variable               | Variable Selector | _(empty)_    | Browse and pick a Control4 system variable                   |
| Add Variable As               | List              | `(Select)`   | `Field` or `Tag`                                             |
| Configured Variables          | String (RO)       | _(auto)_     | Variables in the selected measurement                        |
| Remove Variable               | Dynamic List      | _(auto)_     | Select a variable to remove from the measurement             |
| Remove Measurement            | List              | `(Select)`   | Set to `Remove` to delete the selected measurement           |
| Measurement Write Interval    | List              | `Default`    | Per-measurement override; `Default` uses the global interval |
| Measurement Enabled           | List              | `On`         | `On` or `Off` — pauses writes without deleting configuration |

## Actions Reference

| Action               | Description                                                              |
| -------------------- | ------------------------------------------------------------------------ |
| Test Connection      | Verify connectivity to the InfluxDB health endpoint                      |
| Add Measurement      | Create a new measurement from the value in Measurement Name              |
| Flush Buffer         | Immediately write the current in-memory write buffer to InfluxDB         |
| Force Flush All      | Flush write buffer plus any offline buffer points                        |
| Drain Offline Buffer | Attempt to send buffered offline points to InfluxDB                      |
| Clear Offline Buffer | Discard all offline buffer points (data is lost)                         |
| Update Drivers       | Check GitHub Releases for a newer driver version and update if available |

## Events Reference

| Event           | Fires when…                                                            |
| --------------- | ---------------------------------------------------------------------- |
| Connected       | Driver successfully reaches the InfluxDB health endpoint               |
| Disconnected    | Driver loses connectivity to InfluxDB                                  |
| Write Error     | A batch write returns a non-2xx HTTP response                          |
| Buffer Full     | The offline buffer reaches the Max Buffer Size limit                   |
| Extended Outage | Driver has been disconnected longer than Outage Notification Threshold |

## Conditionals

| Conditional        | Type    | Description                             |
| ------------------ | ------- | --------------------------------------- |
| INFLUXDB_CONNECTED | Boolean | True when connected, False when offline |

Use this in Control4 programming to gate actions on InfluxDB availability.

## Grafana Integration

### Recommended Data Source

Use the **InfluxDB 3** (SQL) data source plugin in Grafana. This uses standard
SQL over Apache Arrow Flight SQL and works with self-hosted InfluxDB 3 OSS and
InfluxDB Cloud.

**Setup:**

1.  In Grafana, go to **Connections → Data Sources → Add data source**.
2.  Search for **InfluxDB** and select the **InfluxDB 3 (SQL)** variant.
3.  Set **Host** to your InfluxDB URL (e.g. `http://192.168.1.100:8086`).
4.  Set **Database** to the same database name configured in the driver.
5.  Paste the same API Token (needs **read** permission on the database).
6.  Click **Save & Test**.

### Example Queries

All queries use InfluxDB 3 SQL syntax (standard SQL, not InfluxQL or Flux).

**Current temperature per thermostat:**

```sql
SELECT
  time,
  temperature
FROM thermostat
WHERE time >= now() - INTERVAL '1 hour'
ORDER BY time ASC
```

**HVAC mode over time:**

```sql
SELECT time, hvac_mode
FROM thermostat
WHERE time >= now() - INTERVAL '24 hours'
ORDER BY time ASC
```

**Lighting level history:**

```sql
SELECT time, level
FROM living_room_dimmer
WHERE time >= now() - INTERVAL '7 days'
ORDER BY time ASC
```

**Latest value per measurement (current state):**

```sql
SELECT last(temperature) AS current_temp, last(humidity) AS current_humidity
FROM thermostat
```

### Starter Dashboards

The `dashboards/` directory in this repository contains pre-built Grafana
dashboard JSON files ready for import.

| File                              | Contents                                             |
| --------------------------------- | ---------------------------------------------------- |
| `dashboards/hvac-monitoring.json` | HVAC temperature, humidity, setpoints, mode, runtime |
| `dashboards/system-health.json`   | Driver write rate, buffer status, error rate         |

**To import:** In Grafana, go to **Dashboards → New → Import**, click **Upload
dashboard JSON file**, and select the file. Choose your InfluxDB data source
when prompted.

After import, update the measurement names in each panel query to match your
actual measurement names.

## Troubleshooting

| Symptom                           | Likely cause & fix                                                                                          |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Status shows "Not tested"         | Click **Test Connection** after setting URL, token, and database                                            |
| Connection fails                  | Verify InfluxDB URL is reachable from the controller; check firewall rules                                  |
| Auth error (HTTP 401)             | API token is missing or does not have write permission on the target database                               |
| No data in Grafana                | Confirm measurement is **Enabled**, at least one variable is configured, and the write interval has elapsed |
| Offline Buffer Size keeps growing | InfluxDB is unreachable; check network and InfluxDB service status; use **Drain Offline Buffer** to retry   |
| HTTP 422 on write                 | Line protocol parse error — often a string value in a numeric field; check variable types                   |
| Data stops after Control4 restart | Expected — driver reloads on restart and reconnects automatically                                           |
| Grafana shows "No data"           | Verify Grafana data source is pointed at the correct database; check time range and query                   |

## Changelog

See
[CHANGELOG.md](https://github.com/finitelabs/control4-influxdb/blob/main/CHANGELOG.md)
for the full release history.
