#!/usr/bin/env python3
"""
 @author Johannes Aalto
 SPDX-License-Identifier: Apache-2.0

 Modified by: Kyle Mishanec
"""

import os
import warnings

import pandas as pd
import requests

from typing import Iterable, Dict, List
from influxdb_client import InfluxDBClient
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction)


try:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=".env")
except ImportError:
    pass


def get_tag_cols(dataframe_keys: Iterable) -> Iterable:
    """Filter out dataframe keys that are not tags"""
    return (
        k
        for k in dataframe_keys
        if not k.startswith("_") and k not in ["result", "table"]
    )


def escape_tag_value(series: pd.Series) -> pd.Series:
    """Escape special characters in tag values per InfluxDB line protocol."""
    return series.astype(str).str.replace(" ", "\\ ", regex=False).str.replace(",", "\\,", regex=False).str.replace("=", "\\=", regex=False)


def get_influxdb_lines(df: pd.DataFrame) -> str:
    """
    Convert the Pandas Dataframe into InfluxDB line protocol.

    The dataframe should be similar to results received from query_api.query_data_frame()

    Not quite sure if this supports all kinds if InfluxDB schemas.
    It might be that influxdb_client package could be used as an alternative to this,
    but I'm not sure about the authorizations and such.

    Protocol description: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
    """
    line = escape_tag_value(df["_measurement"])

    for col_name in get_tag_cols(df):
        line += ("," + col_name + "=") + escape_tag_value(df[col_name])

    line += (
        " "
        + df["_field"]
        + "="
        + df["_value"].astype(str)
        + " "
        + df["_time"].astype(int).astype(str)
    )
    return "\n".join(line)


def main(args: Dict[str, str]):
    print("args: " + str(args.keys()))
    bucket = args.pop("bucket")
    url = args.pop("vm_addr")
    start = int(args.pop("start"))
    end = int(args.pop("end"))
    chunk_hours = int(args.pop("chunk_hours"))

    for k, v in args.items():
        if v is not None:
            os.environ[k] = v
        print(f"Using {k}={os.getenv(k)}")

    chunk_seconds = chunk_hours * 3600
    total_seconds = end - start
    total_chunks = (total_seconds + chunk_seconds - 1) // chunk_seconds
    print(f"Time range: start={start}, stop={end} ({total_seconds}s, {total_chunks} chunks of {chunk_hours}h)")

    client = InfluxDBClient.from_env_properties()

    query_api = client.query_api()  # use synchronous to see errors

    # Get all unique series by reading first entry of every table.
    # With latest InfluxDB we could possibly use "schema.measurements()" but this doesn't exist in 2.0
    first_in_series = f"""
    from(bucket: "{bucket}")
    |> range(start: {start}, stop: {end})
    |> first()"""
    timeseries: List[pd.DataFrame] = query_api.query_data_frame(first_in_series)

    if isinstance(timeseries, pd.DataFrame):
        timeseries = [timeseries]

    # get all unique measurement-field pairs and then fetch and export them one-by-one.
    measurements_and_fields = [
        gr[0] for df in timeseries for gr in df.groupby(["_measurement", "_field"])
    ]
    print(f"Found {len(measurements_and_fields)} unique time series")
    for meas, field in measurements_and_fields:
        print(f"Exporting {meas}_{field}")

        chunk_start = start
        chunk_num = 0
        while chunk_start < end:
            chunk_stop = min(chunk_start + chunk_seconds, end)
            chunk_num += 1
            print(f"  Chunk {chunk_num}/{total_chunks}: {chunk_start} -> {chunk_stop}")

            whole_series = f"""
            from(bucket: "{bucket}")
            |> range(start: {chunk_start}, stop: {chunk_stop})
            |> filter(fn: (r) => r["_measurement"] == "{meas}")
            |> filter(fn: (r) => r["_field"] == "{field}")
            """
            df = query_api.query_data_frame(whole_series)
            if isinstance(df, list):
                df = pd.concat(df, ignore_index=True)

            if df.empty or "_measurement" not in df.columns:
                chunk_start = chunk_stop
                continue

            line = get_influxdb_lines(df)
            # "db" is added as an extra tag for the value.
            resp = requests.post(f"{url}/influx/api/v2/write", data=line)
            if resp.status_code != 204:
                print(f"    Warning: write returned {resp.status_code}: {resp.text}")

            chunk_start = chunk_stop

        print(f"  Done exporting {meas}_{field}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Script for exporting InfluxDB data into victoria metrics instance. \n"
        " InfluxDB settings can be defined on command line or as environment variables"
        " (or in .env file if python-dotenv is installed)."
        " InfluxDB related args described in \n"
        "https://github.com/influxdata/influxdb-client-python#via-environment-properties"
    )
    parser.add_argument(
        "bucket",
        type=str,
        help="InfluxDB source bucket",
    )
    parser.add_argument(
        "--INFLUXDB_V2_ORG",
        "-o",
        type=str,
        help="InfluxDB organization",
    )
    parser.add_argument(
        "--INFLUXDB_V2_URL",
        "-u",
        type=str,
        help="InfluxDB Server URL, e.g., http://localhost:8086",
    )
    parser.add_argument(
        "--INFLUXDB_V2_TOKEN",
        "-t",
        type=str,
        help="InfluxDB access token.",
    )
    parser.add_argument(
        "--INFLUXDB_V2_SSL_CA_CERT",
        "-S",
        type=str,
        help="Server SSL Cert",
    )
    parser.add_argument(
        "--INFLUXDB_V2_TIMEOUT",
        "-T",
        type=str,
        help="InfluxDB timeout",
    )
    parser.add_argument(
        "--INFLUXDB_V2_VERIFY_SSL",
        "-V",
        type=str,
        help="Verify SSL CERT.",
    )

    parser.add_argument(
        "--vm-addr",
        "-a",
        type=str,
        help="VictoriaMetrics server",
    )
    parser.add_argument(
        "--start",
        "-s",
        type=int,
        required=True,
        help="Range start as unix timestamp (e.g., 1641013200)",
    )
    parser.add_argument(
        "--end",
        "-e",
        type=int,
        required=True,
        help="Range stop as unix timestamp (e.g., 1672549199)",
    )
    parser.add_argument(
        "--chunk-hours",
        "-c",
        type=int,
        default=1,
        help="Hours per chunk for writing to VictoriaMetrics (default: 1)",
    )
    main(vars(parser.parse_args()))
