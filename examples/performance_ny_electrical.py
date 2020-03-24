"""
Built a time series database for 11 electrical zones of the state New York. The Time Series starts on Jan 1st 2006
and continuous until Feb 29th 2020 in 5 minutes steps.

Query to measure: Electric load in Zone by Time range

https://github.com/influxdata/influxdb-client-python/issues/78
"""

import datetime
import random
import timeit
from typing import List

import ciso8601
import rx
from rx import operators as ops

from influxdb_client import Point, InfluxDBClient, WriteOptions, WritePrecision

path = "http://localhost:9999"
token = "my-token"
bucket = "my-bucket"
org = "my-org"

start = ciso8601.parse_datetime('2006-01-01 00:00:00')
states = ["CAPITL", "CENTRL", "DUNWOD", "GENESE", "HUD", "LONGIL", "MHK", "MILLWD", "N.Y.C.", "NORTH", "WEST"]


def _create_point(dt: datetime, state: str) -> Point:
    """
    Create Point for one zone
    """
    return Point("NYISO_Data") \
        .tag("Zone", state) \
        .field("Load", round(random.uniform(200, 6000), 1)) \
        .time(dt, write_precision=WritePrecision.S)


def _create_points(index) -> List[Point]:
    """
    Create Points for all zones
    """
    dt = start + datetime.timedelta(minutes=index * 5)
    if index % 1000 == 0:
        print(dt)
    points = map(lambda state: _create_point(dt, state), states)
    return list(points)


client = InfluxDBClient(url=path, token=token, org=org, debug=False)

"""
Importing data
"""
if False:
    data = rx.range(0, 14 * 365 * 24 * 12).pipe(ops.map(lambda index: _create_points(index)))

    write_api = client.write_api(write_options=WriteOptions(batch_size=50_000))
    write_api.write(org=org, bucket=bucket, record=data, write_precision=WritePrecision.S)
    write_api.__del__()


"""
Measuring how long the query takes
"""
query = f'from(bucket: "{bucket}")' \
        '|> range(start: -13mo, stop: -1mo)' \
        '|> filter(fn: (r) => r._measurement == "NYISO_Data")' \
        '|> filter(fn: (r) => r.Zone == "DUNWOD")'


def _query():
    result = client.query_api().query(org=org, query=query)
    print(f"Tables count: {len(result)}, results count: {len(result[0].records)}")
    pass


print(f"Query takes: {timeit.timeit(_query, number=1)} [sec]")

"""
Close client
"""
client.__del__()
