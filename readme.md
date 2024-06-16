# Go Client supporting Exporter schemas provided by OpenTelemetry

This project is a Work in progress and only Supports ClickHouse at this time.
The Sum and Gauge Metrics queries are accurate and have been validated against
PromQL.

## ClickHouse Exporter

This project provides a Query Builder for Sum and Gauge Metrics that are compatible with the ClickHouse schema produced by [Clickhouse Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter). It calculates the Increase for Sum Metrics following the patterns established in the [Altinity Grafana DataSource](https://grafana.com/grafana/plugins/vertamedia-clickhouse-datasource/) but is more optimized using Windowing.
