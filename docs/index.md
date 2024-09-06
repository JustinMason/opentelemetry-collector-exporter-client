# Clickhouse Exporter provided by OpenTelemetry

## Utilities

### Logical Materialized View Pattern

Materialized views are great ways to aggregate data as its
written to the root table.  Creating new Materialized views
for existing tables enables new aggregation use cases.  The
challenge can be populating the new Materialized view with historical
data already written to the root table.  Clickhouse has limits on
buffer sizes that can cause timeouts and max thresholds to prevent
a single select into from replaying data through a Materialized view.
This following python script was created to support this capability.
It chunks the `select into insert` into time slices to allow parallel
processing of copying data from one large table into another.

```python
from clickhouse_driver import Client
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import argparse

def insert_rows(rangeArgs):
    time_range, args = rangeArgs

    delta = timedelta(hours=1)
    start_time = time.time()
    # Connect to your ClickHouse DB
    client = Client(args.host, user='ci_migrations', password=args.password, port=args.port, secure=False, verify=False, database='otel')

    query = f"""
    INSERT INTO {args.table_to}
    SELECT ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl,
    MetricName,
    MetricDescription, MetricUnit, `Attributes`, StartTimeUnix, TimeUnix, Value, Flags, `Exemplars.FilteredAttributes`, `Exemplars.TimeUnix`, `Exemplars.Value`, `Exemplars.SpanId`, `Exemplars.TraceId`, AggTemp, IsMonotonic, null
    FROM {args.table_from}
    WHERE TimeUnix BETWEEN '{time_range}' AND '{time_range+delta}'    
    AND MetricName in ('prometheus_http_requests_total') """

    print(query)
    client.execute(query)

    end_time = time.time()  # End timing
    elapsed_time = end_time - start_time 
    print(f"Inserted BETWEEN '{time_range}' AND '{time_range+delta}' in {elapsed_time} seconds")

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def parse_datetime(s):
    return datetime.strptime(s,'%Y-%m-%dT%H:%M:%S')

def main():

    parser = argparse.ArgumentParser(description='Select into ClickHouse DB.')
    parser.add_argument('--host', type=str, help='Host for the ClickHouse DB')
    parser.add_argument('--port', type=int, help='Port for the ClickHouse DB')
    parser.add_argument('--user', type=str, help='user for the ClickHouse DB')
    parser.add_argument('--password', type=str, help='Password for the ClickHouse DB')
    parser.add_argument('--start', type=parse_datetime, help='Start datetime for select into')
    parser.add_argument('--end', type=parse_datetime, help='End datetime for select into')
    parser.add_argument('--concurrent', type=int, help='Concurrent thread pool')
    parser.add_argument('--table-from', type=str, help='Select from table')
    parser.add_argument('--table-to', type=str, help='Insert into table')
    
    args = parser.parse_args()

    print("host", args.host)
    print("port", args.port)
    print("user", args.user)
    print("password", args.password[:4] + '*' * (len(args.password) - 4))
    print("start", args.start)
    print("end", args.end)
    print("concurrent", args.concurrent)    
    print("table-from", args.table_from)
    print("table-to", args.table_to)

    delta = timedelta(hours=1)

    total_start_time = time.time()

    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
        qryRange = datetime_range(args.start, args.end, delta)
        executor.map(insert_rows, [(time_range, args) for time_range in qryRange])

    total_end_time = time.time()

    # # Calculate the total elapsed time
    total_elapsed_time = total_end_time - total_start_time

    print(f"Total time taken: {total_elapsed_time} seconds")

if __name__ == '__main__':
    main()
```
