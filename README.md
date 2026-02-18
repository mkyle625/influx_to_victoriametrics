
Script to import data from InfluxDB >=2.0 to VictoriaMetrics.

https://github.com/VictoriaMetrics/vmctl provides similar features for InfluxDB 1.X.

Every unique timeseries is queried one by one and exported to VictoriaMetrics.
Data is automatically chunked into configurable time windows (default: 1 hour) to avoid exceeding VictoriaMetrics' request size limits.

## Usage

```
python influx_export.py my_bucket \
  -a http://victoriametrics:8428 \
  --start 1641013200 \
  --end 1672549199
```

### Arguments

```
positional arguments:
  bucket                InfluxDB source bucket

required arguments:
  --start, -s           Range start as unix timestamp (e.g., 1641013200)
  --end, -e             Range stop as unix timestamp (e.g., 1672549199)

optional arguments:
  -h, --help            show this help message and exit
  --vm-addr, -a         VictoriaMetrics server URL
  --chunk-hours, -c     Hours per chunk for writing to VictoriaMetrics (default: 1)
  --INFLUXDB_V2_ORG, -o
                        InfluxDB organization
  --INFLUXDB_V2_URL, -u
                        InfluxDB Server URL, e.g., http://localhost:8086
  --INFLUXDB_V2_TOKEN, -t
                        InfluxDB access token
  --INFLUXDB_V2_SSL_CA_CERT, -S
                        Server SSL Cert
  --INFLUXDB_V2_TIMEOUT, -T
                        InfluxDB timeout
  --INFLUXDB_V2_VERIFY_SSL, -V
                        Verify SSL CERT
```

InfluxDB settings can also be defined as environment variables or in a `.env` file (if `python-dotenv` is installed). See [influxdb-client-python docs](https://github.com/influxdata/influxdb-client-python#via-environment-properties) for details.

### Chunking

Large datasets will exceed VictoriaMetrics' request size limit. The `--chunk-hours` flag controls how much data is sent per request. The default of 1 hour works well for most cases. If you still hit 413 errors, try a smaller value. If exports are slow, try a larger value like 6.

```
# 6-hour chunks for faster exports with smaller datasets
python influx_export.py my_bucket -a http://vm:8428 --start 1641013200 --end 1672549199 -c 6
```

## Original Author

Johannes Aalto

SPDX-License-Identifier: Apache-2.0
