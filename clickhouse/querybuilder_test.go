package clickhouse

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var expectedSumGrpSQL = "\n\n\n\nSELECT attr_1,\nUsageTime, sum(Usage) Usage\nFROM ( \n\nSELECT  arrayElement(splitByString(':', increaseKey), 1) AS attr_1, arrayElement(splitByString(':', increaseKey), 2) AS attr_2, arrayElement(splitByString(':', increaseKey), 3) AS attr_3, arrayElement(splitByString(':', increaseKey), 4) AS attr_4, arrayElement(splitByString(':', increaseKey), 5) AS attr_5,\n  toDateTime(intDiv(toUInt32(TimeUnix), 300) * 300) AS UsageTime,\n  sum(IncreaseValue) as Usage\nFROM (\n    SELECT concat(Attributes['attr_1'] ,':',  Attributes['attr_2'] ,':',  Attributes['attr_3'] ,':',  Attributes['attr_4'] ,':',  Attributes['attr_5']  ) as increaseKey,\n    TimeUnix,\n\tMetricName,\n    lagInFrame(Value) OVER (PARTITION BY increaseKey ORDER BY TimeUnix ASC ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS prevValue,\n\t0 Mark,\n\tCOUNT(Mark) OVER (PARTITION BY increaseKey ORDER BY\tTimeUnix ROWS 1 PRECEDING)-1 = 1 PrevExists,\n\tif(PrevExists,\n\t    if( prevValue > Value,\n\t\t\tif(prevValue = 0,\n\t\t\t    0,\n\t\t\t    Value),\n\t\tValue - prevValue),\n\t0) as IncreaseValue\n    FROM otel_metrics_local_sum_5m\n    WHERE MetricName = 'metric_name'\n\t    AND NOT isNaN(Value)\n         AND Attributes['attr_2'] = 'id_1'  AND Attributes['attr_3'] = 'id_3'  AND Attributes['attr_4'] = '0' \n        AND TimeUnix BETWEEN (toDateTime('2024-05-01 00:00:00') - INTERVAL 300 SECOND) AND toDateTime('2024-05-02 00:00:00') ) AS data\nGROUP BY\n\tincreaseKey,\n\tUsageTime\nORDER BY\n\tincreaseKey,\n\tUsageTime\n\n\n) as grouped \nGROUP BY UsageTime,\n    \n        attr_1  \nORDER BY attr_1,\nUsageTime"
var expectedSumNoGroupSQL = "\n\n\n\n\nSELECT  arrayElement(splitByString(':', increaseKey), 1) AS attr_1, arrayElement(splitByString(':', increaseKey), 2) AS attr_2, arrayElement(splitByString(':', increaseKey), 3) AS attr_3, arrayElement(splitByString(':', increaseKey), 4) AS attr_4, arrayElement(splitByString(':', increaseKey), 5) AS attr_5,\n  toDateTime(intDiv(toUInt32(TimeUnix), 300) * 300) AS UsageTime,\n  sum(IncreaseValue) as Usage\nFROM (\n    SELECT concat(Attributes['attr_1'] ,':',  Attributes['attr_2'] ,':',  Attributes['attr_3'] ,':',  Attributes['attr_4'] ,':',  Attributes['attr_5']  ) as increaseKey,\n    TimeUnix,\n\tMetricName,\n    lagInFrame(Value) OVER (PARTITION BY increaseKey ORDER BY TimeUnix ASC ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS prevValue,\n\t0 Mark,\n\tCOUNT(Mark) OVER (PARTITION BY increaseKey ORDER BY\tTimeUnix ROWS 1 PRECEDING)-1 = 1 PrevExists,\n\tif(PrevExists,\n\t    if( prevValue > Value,\n\t\t\tif(prevValue = 0,\n\t\t\t    0,\n\t\t\t    Value),\n\t\tValue - prevValue),\n\t0) as IncreaseValue\n    FROM otel_metrics_local_sum_5m\n    WHERE MetricName = 'metric_name'\n\t    AND NOT isNaN(Value)\n         AND Attributes['attr_2'] = 'id_1'  AND Attributes['attr_3'] = 'id_3'  AND Attributes['attr_4'] = '0' \n        AND TimeUnix BETWEEN (toDateTime('2024-05-01 00:00:00') - INTERVAL 300 SECOND) AND toDateTime('2024-05-02 00:00:00') ) AS data\nGROUP BY\n\tincreaseKey,\n\tUsageTime\nORDER BY\n\tincreaseKey,\n\tUsageTime\n\n"

var expectedGaugeGrpSQL = "\n\n\n\nSELECT attr_1,\nUsageTime, sum(Usage) Usage\nFROM ( \nSELECT Attributes['attr_1'] as attr_1, Attributes['attr_2'] as attr_2, Attributes['attr_3'] as attr_3, \ntoDateTime(intDiv(toUInt32(TimeUnix), 300) * 300) AS UsageTime,\navg(Value)/1e6 as Usage\nFROM otel.otel_metrics_local_sum_5m\nWHERE MetricName = 'gauge_metric_name'\n\tAND NOT isNaN(Value)\n     AND Attributes['attr_2'] = 'id_2'  AND match(Attributes['attr_3'] ,'.*?\\-\\d+') \n    AND TimeUnix BETWEEN (toDateTime('2024-05-12 18:15:02') - INTERVAL 300 SECOND) AND toDateTime('2024-05-13 18:15:02')\nGROUP BY UsageTime, attr_1,attr_2,attr_3\nORDER BY UsageTime\n\n\n) as grouped \nGROUP BY UsageTime,\n    \n        attr_1  \nORDER BY attr_1,\nUsageTime"
var expectedGaugeNoGroupSQL = "\n\n\n\nSELECT Attributes['attr_1'] as attr_1, Attributes['attr_2'] as attr_2, Attributes['attr_3'] as attr_3, \ntoDateTime(intDiv(toUInt32(TimeUnix), 300) * 300) AS UsageTime,\navg(Value)/1e6 as Usage\nFROM otel.otel_metrics_local_sum_5m\nWHERE MetricName = 'gauge_metric_name'\n\tAND NOT isNaN(Value)\n     AND Attributes['attr_2'] = 'id_2'  AND match(Attributes['attr_3'] ,'.*?\\-\\d+') \n    AND TimeUnix BETWEEN (toDateTime('2024-05-12 18:15:02') - INTERVAL 300 SECOND) AND toDateTime('2024-05-13 18:15:02')\nGROUP BY UsageTime, attr_1,attr_2,attr_3\nORDER BY UsageTime\n\n"

func TestMetricSumGroupSQLBuilder(t *testing.T) {

	var start, _ = time.Parse(time.RFC3339, "2024-05-01T00:00:00Z")
	var end, _ = time.Parse(time.RFC3339, "2024-05-02T00:00:00Z")

	builder := NewSumMetricSQLBuilder()
	assert.NotNil(t, builder, "Expected non-nil builder instance")
	builder.Select("attr_1", "attr_2", "attr_3", "attr_4", "attr_5")
	builder.From("otel_metrics_local_sum_5m")
	builder.MetricName("metric_name")
	builder.Where("AND Attributes['attr_2'] = 'id_1'", "AND Attributes['attr_3'] = 'id_3'", "AND Attributes['attr_4'] = '0'")
	builder.Range(start, end)
	builder.Group("attr_1")
	builder.Interval(300)

	sql, err := builder.Build()

	assert.Nil(t, err, "Expected error to be nil")
	assert.Equal(t, expectedSumGrpSQL, sql, "Expected SUM Group SQL statement to match")
}

func TestMetricSumNoGroupSQLBuilder(t *testing.T) {

	var start, _ = time.Parse(time.RFC3339, "2024-05-01T00:00:00Z")
	var end, _ = time.Parse(time.RFC3339, "2024-05-02T00:00:00Z")

	builder := NewSumMetricSQLBuilder()
	assert.NotNil(t, builder, "Expected non-nil builder instance")
	builder.Select("attr_1", "attr_2", "attr_3", "attr_4", "attr_5")
	builder.From("otel_metrics_local_sum_5m")
	builder.MetricName("metric_name")
	builder.Where("AND Attributes['attr_2'] = 'id_1'", "AND Attributes['attr_3'] = 'id_3'", "AND Attributes['attr_4'] = '0'")
	builder.Range(start, end)
	builder.Interval(300)

	sql, err := builder.Build()
	assert.Nil(t, err, "Expected error to be nil")
	fmt.Print(sql)
	assert.Equal(t, expectedSumNoGroupSQL, sql, "Expected SUM No Group SQL statement to match")
}

func TestMetricGaugeGroupBySQLBuilder(t *testing.T) {

	var start, _ = time.Parse(time.RFC3339, "2024-05-12T18:15:02Z")
	var end, _ = time.Parse(time.RFC3339, "2024-05-13T18:15:02Z")

	builder := NewGaugeMetricSQLBuilder()
	assert.NotNil(t, builder, "Expected non-nil builder instance")
	builder.Select("attr_1", "attr_2", "attr_3")
	builder.From("otel.otel_metrics_local_sum_5m")
	builder.MetricName("gauge_metric_name")
	builder.Where("AND Attributes['attr_2'] = 'id_2'", "AND match(Attributes['attr_3'] ,'.*?\\-\\d+')")
	builder.Range(start, end)
	builder.Group("attr_1")
	builder.Interval(300)

	sql, err := builder.Build()
	assert.Nil(t, err, "Expected error to be nil")
	fmt.Print(sql)
	assert.Equal(t, expectedGaugeGrpSQL, sql, "Expected Gauge Group SQL statement to match")
}

func TestMetricGaugeNoGroupBySQLBuilder(t *testing.T) {

	var start, _ = time.Parse(time.RFC3339, "2024-05-12T18:15:02Z")
	var end, _ = time.Parse(time.RFC3339, "2024-05-13T18:15:02Z")

	builder := NewGaugeMetricSQLBuilder()
	assert.NotNil(t, builder, "Expected non-nil builder instance")
	builder.Select("attr_1", "attr_2", "attr_3")
	builder.From("otel.otel_metrics_local_sum_5m")
	builder.MetricName("gauge_metric_name")
	builder.Where("AND Attributes['attr_2'] = 'id_2'", "AND match(Attributes['attr_3'] ,'.*?\\-\\d+')")
	builder.Range(start, end)
	builder.Interval(300)

	sql, err := builder.Build()
	assert.Nil(t, err, "Expected error to be nil")
	fmt.Print(sql)
	assert.Equal(t, expectedGaugeNoGroupSQL, sql, "Expected Gauge No Group SQL statement to match")
}

func Test_sqlBuilder_validateBuilder(t *testing.T) {

	tests := []struct {
		name       string
		from       string
		selectMeth []string
		whereMeth  []string
		rangeFrom  time.Time
		rangeTo    time.Time
		groupMeth  []string
		interval   int
		metricName string
		err        error
	}{
		{
			name: "Valid FROM table",
			from: "",
			err:  fmt.Errorf("FROM table is required"),
		},
		{
			name: "Valid SELECT",
			from: "from_tbl",
			err:  fmt.Errorf("SELECT columns are required"),
		},
		{
			name:       "Valid Metric Name",
			from:       "from_tbl",
			selectMeth: []string{"col1", "col2"},
			err:        fmt.Errorf("Metric name is required"),
		},
		{
			name:       "Valid Interval",
			from:       "from_tbl",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			err:        fmt.Errorf("Interval is required"),
		},
		{
			name:       "Valid Interval size",
			from:       "from_tbl",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   59,
			err:        fmt.Errorf("Interval must be at least 60 seconds"),
		},
		{
			name:       "Valid Start",
			from:       "from_tbl",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			err:        fmt.Errorf("start time is required"),
		},
		{
			name:       "Valid End",
			from:       "from_tbl",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			err:        fmt.Errorf("end time is required"),
		},
		{
			name:       "Valid Group name",
			from:       "from_tbl",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * 1),
			groupMeth:  []string{""},
			err:        fmt.Errorf("Group column name can not be empty"),
		},
		{
			name:       "Valid Group existing in select column",
			from:       "from_tbl",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * 1),
			groupMeth:  []string{"col3"},
			err:        fmt.Errorf("Group column col3 is not in SELECT columns"),
		},
		{
			name:       "Invalid end range",
			from:       "from_tbl",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * -1),
			groupMeth:  []string{"col1"},
			err:        fmt.Errorf("Range invalid, 'end' cannot be less than 'start'"),
		},
		{
			name:       "Valid Range",
			from:       "from_tbl",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * 2),
			groupMeth:  []string{"col1"},
		},
		{
			name:       "SQL Injection TERMINATE Statement",
			from:       ";",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * 1),
			groupMeth:  []string{"col1"},
			err:        fmt.Errorf("SQL statement contains dangerous keywords"),
		},
		{
			name:       "SQL Injection DELETE",
			from:       "from_tbl DELETE FROM table",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * 1),
			groupMeth:  []string{"col1"},
			err:        fmt.Errorf("SQL statement contains dangerous keywords"),
		},
		{
			name:       "SQL Injection DROP",
			from:       "from_tbl DROP table",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * 1),
			groupMeth:  []string{"col1"},
			err:        fmt.Errorf("SQL statement contains dangerous keywords"),
		},
		{
			name:       "SQL Injection INSERT",
			from:       "from_tbl INSERT INTO table;",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * 1),
			groupMeth:  []string{"col1"},
			err:        fmt.Errorf("SQL statement contains dangerous keywords"),
		},
		{
			name:       "SQL Injection TRUNCATE",
			from:       "TRUNCATE",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * 1),
			groupMeth:  []string{"col1"},
			err:        fmt.Errorf("SQL statement contains dangerous keywords"),
		},
		{
			name:       "SQL Injection CREATE",
			from:       "CREATE",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * 1),
			groupMeth:  []string{"col1"},
			err:        fmt.Errorf("SQL statement contains dangerous keywords"),
		},
		{
			name:       "SQL Injection UPDATE",
			from:       "Update",
			selectMeth: []string{"col1", "col2"},
			metricName: "metric_name",
			interval:   300,
			rangeFrom:  time.Now(),
			rangeTo:    time.Now().Add(time.Hour * 1),
			groupMeth:  []string{"col1"},
			err:        fmt.Errorf("SQL statement contains dangerous keywords"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := NewSumMetricSQLBuilder()
			sb.From(tt.from)
			sb.Select(tt.selectMeth...)
			sb.MetricName(tt.metricName)
			sb.Where(tt.whereMeth...)
			sb.Range(tt.rangeFrom, tt.rangeTo)
			sb.Group(tt.groupMeth...)
			sb.Interval(tt.interval)
			_, err := sb.Build()

			if (err != nil && tt.err == nil) || (err == nil && tt.err != nil) || (err != nil && tt.err != nil && err.Error() != tt.err.Error()) {
				t.Errorf("%s NewSQLBuilder() Validation error got = %s, want %s", tt.name, err, tt.err)
			}
		})
	}

}
