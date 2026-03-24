# InfluxDB Data Logger

Logs Control4 variable changes to an InfluxDB time-series database.

## Configuration

| Property | Description |
|----|----|
| InfluxDB URL | Full URL of the InfluxDB instance (e.g. `http://192.168.1.100:8086`) |
| API Token | InfluxDB API authentication token |
| Database | Bucket or database name to write into |
| Write Precision | Timestamp precision for line protocol (`ms` default) |
| Default Write Interval | How often to flush buffered points |

## Actions

- **Test Connection** – verify connectivity to the InfluxDB `/health`
  endpoint
- **Add Measurement** – register the measurement name entered in the
  property field
- **Flush Buffer** – immediately write all buffered points

## Events

| Event        | Fires when…                                              |
|--------------|----------------------------------------------------------|
| Connected    | Driver successfully reaches the InfluxDB health endpoint |
| Disconnected | Driver loses connectivity                                |
| Write Error  | A batch write returns a non-2xx response                 |
| Buffer Full  | The write buffer reaches `MAX_BUFFER_SIZE`               |
