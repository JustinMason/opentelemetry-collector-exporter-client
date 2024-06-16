package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
)

func getConnection() driver.Conn {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{""},
		Auth: clickhouse.Auth{
			Database: "otel",
			Username: "",
			Password: "",
		},
		TLS:   &tls.Config{InsecureSkipVerify: true},
		Debug: true,
		Debugf: func(format string, v ...any) {
			fmt.Printf(format+"\n", v...)
		},
	})

	if err != nil {
		panic(err)
	}

	return conn
}

type SelectResult struct {
	Attr1       string
	Attr2    string
	Attr3       string
	Attr4 string
	Attr5           string
	UsageTime     time.Time
	Usage         float64
}

func TestSQLBuilderExecutionColumnMapping(t *testing.T) {

	var metricType SelectResult

	var start, _ = time.Parse(time.RFC3339, "2024-05-01T00:00:00Z")
	var end, _ = time.Parse(time.RFC3339, "2024-05-02T00:00:00Z")

	builder := NewSumMetricSQLBuilder()
	builder.Select("attr_1", "attr_2", "attr_3", "attr_4", "attr_5")
	builder.From("otel_metrics_local_sum_5m")
	builder.MetricName("metric_name")
	builder.Where("AND Attributes['attr_2'] = 'id_1'", "AND Attributes['attr_3'] = 'id_3'", "AND Attributes['attr_4'] = '0'")
	builder.Range(start, end)
	builder.Interval(300)

	ch := NewClickHouse(context.Background(), getConnection())

	metrics, err := ch.Query(builder, &metricType)

	if err != nil {
		assert.Error(t, err, "Expected error to be nil")
	}

	if metrics == nil {
		assert.Fail(t, "Expected metrics to not be nil")
	}

}

type SelectResultGrouped struct {
	Attr1   string
	UsageTime time.Time
	Usage     float64
}

func TestSQLBuilderExecutionColumnOverageMapping(t *testing.T) {

	var metricType SelectResultGrouped

	var start, _ = time.Parse(time.RFC3339, "2024-05-01T00:00:00Z")
	var end, _ = time.Parse(time.RFC3339, "2024-05-02T00:00:00Z")

	builder := NewSumMetricSQLBuilder()
	builder.Select("attr_1", "attr_2", "attr_3", "attr_4", "attr_5")
	builder.Group("attr_1")
	builder.From("otel_metrics_local_sum_5m")
	builder.MetricName("metric_name")
	builder.Where("AND Attributes['attr_2'] = 'id_1'", "AND Attributes['attr_3'] = 'id_3'", "AND Attributes['attr_4'] = '0'")
	builder.Range(start, end)
	builder.Interval(300)

	ch := NewClickHouse(context.Background(), getConnection())

	metrics, err := ch.Query(builder, &metricType)

	if err != nil {
		assert.Error(t, err, "Expected error to be nil")
	}

	if metrics == nil {
		assert.Fail(t, "Expected metrics to not be nil")
	}

}
