<p><img src="FACTS-STRIP-URL" alt="" width="1200" height="68" /></p>

## Overview

Keep a history of your Control4 project in InfluxDB. The driver logs the
Control4 variables you choose to an InfluxDB 3 database the controller can
reach, so you can chart them and look back over weeks and months.

You set it up in the driver's own Composer tab: group readings into
measurements, pick each device's variable from a searchable list, and see each
value as it will be written. The driver writes in batches and holds what it
cannot send during an outage, sending it once InfluxDB is back.

## Features

- Any Control4 variable logged as an InfluxDB field or tag
- Measurements set up in a Composer tab, with a live preview of each value
- A write interval for each measurement, with unchanged values skipped
- Device and room names added as tags in one click
- Values converted before they are written, with a Lua transform per reading
- Writes held in memory through an outage and sent once InfluxDB is back
- An event when an outage lasts longer than the time you set
- Connection events, a connected conditional and write counts for programming

## System requirements

- Control4 OS 3.3+
- An InfluxDB 3 server the Control4 controller can reach, with a database and an
  API token that can write to it
