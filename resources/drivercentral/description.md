<p><img src="FACTS-STRIP-URL" alt="" width="1200" height="68" /></p>

## Overview

Log Control4 variables to an InfluxDB time-series database. Group the variables
you want into measurements, each with its own write interval, and the driver
writes them in batches, holding them through an outage and retrying until the
server is back.

## Features

- Any Control4 variable logged as a field or a tag
- Measurements with their own write intervals
- Timestamp precision from seconds to nanoseconds
- Writes held in memory through an outage, within a fixed limit, and retried
  until the server answers again
- An event when an outage runs longer than the time you set
- Connection status events and conditionals for programming
- A Composer tab to set up measurements and see the driver's status

## System requirements

- Control4 OS 3.3+
- An InfluxDB 3 server the Control4 controller can reach, with an API token that
  can write to it
