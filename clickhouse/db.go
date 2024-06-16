package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type clickHouse struct {
	connection driver.Conn
	dialCount  int
	context    context.Context
}

func NewClickHouse(ctx context.Context,
	connection driver.Conn) *clickHouse {

	instance := &clickHouse{
		dialCount:  0,
		context:    ctx,
		connection: connection,
	}

	return instance
}

// builder: The SQLBuilder that will be used to build the SQL query.
//
// otelResultInterface: An interface for the OpenTelemetry result. Note the last 2 fields of the struct are required to be `UsageTime time.Time` &	`Usage float64“
// The initial fields should align with the SQL query that is being executed.  Either the Select if no Group, or the Group fields from the SQLBuilder.
//
// Returns a array of metricdata.Metrics and an error. If there is an issue with building the SQL query or executing it,
// it will return an error.
//
//	type SelectResultGrouped struct {
//		Attr1   string
//		UsageTime time.Time
//		Usage     float64
//	}
func (c *clickHouse) Query(builder SQLBuilder, otelResultInterface interface{}) ([]metricdata.Metrics, error) {

	otelMetrics := make([]metricdata.Metrics, 0)

	sql, err := builder.Build()
	if err != nil {
		return nil, err
	}

	rows, err := c.connection.Query(c.context, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	//Use columns to create attributes for metric
	columns := rows.Columns()

	val := reflect.ValueOf(otelResultInterface).Elem()
	values := make([]interface{}, len(columns))
	valuePointers := make([]interface{}, len(columns))

	for i := range columns {
		values[i] = reflect.New(val.Field(i).Type()).Interface()
		valuePointers[i] = values[i]
	}

	var points []metricdata.DataPoint[float64]

	for rows.Next() {
		//Populate pointers from Database
		if err := rows.Scan(valuePointers...); err != nil {
			return nil, err
		}

		//Set val fields with the pointer values
		for i := range columns {
			val.Field(i).Set(reflect.ValueOf(values[i]).Elem())
		}

		// Load Attributes from Columns
		keyValues := []attribute.KeyValue{}

		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			fieldType := val.Type().Field(i)

			// Extract the field name and value.
			fieldName := fieldType.Name
			fieldValue := fmt.Sprintf("%v", field.Interface())

			if fieldName == "Usage" || fieldName == "UsageTime" || fieldName == "Metric" {
				continue
			}
			// Create an attribute.KeyValue and append it to the slice.
			//lowercase attribute names
			keyValue := attribute.String(strings.ToLower(fieldName), fieldValue)
			keyValues = append(keyValues, keyValue)
		}

		attributeSet := attribute.NewSet(keyValues...)

		var usageTime time.Time

		usageTimeReflected := val.FieldByName("UsageTime")
		if usageTimeReflected.IsValid() {
			if usageTimeReflected.Type() == reflect.TypeOf(time.Time{}) {
				usageTime = usageTimeReflected.Interface().(time.Time)
			}
		} else {
			return nil, errors.New("UsageTime not valid time.Time Type")
		}

		var usage float64

		usageReflect := val.FieldByName("Usage")
		if usageReflect.IsValid() {
			if usageReflect.Kind() == reflect.Float64 {
				usage = usageReflect.Float()
			}
		}

		points = append(points, metricdata.DataPoint[float64]{
			Time:       usageTime,
			StartTime:  usageTime,
			Value:      usage,
			Attributes: attributeSet,
		})

	}

	otelMetrics = append(otelMetrics, metricdata.Metrics{
		Name:        builder.GetMetricName(),
		Description: "",
		Unit:        "by",
		Data:        metricdata.Sum[float64]{DataPoints: points, Temporality: metricdata.CumulativeTemporality, IsMonotonic: true},
	})

	return otelMetrics, nil

}
