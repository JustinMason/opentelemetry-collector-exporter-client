package clickhouse

import (
	"bytes"
	"fmt"
	"regexp"
	"text/template"
	"time"
)

// SQLBuilder is the interface for building SQL statements.
type SQLBuilder interface {
	MetricName(name string) SQLBuilder
	GetMetricName() string
	Select(columns ...string) SQLBuilder
	From(table string) SQLBuilder
	Where(condition ...string) SQLBuilder
	Range(start, end time.Time) SQLBuilder
	Group(groups ...string) SQLBuilder
	Interval(interval int) SQLBuilder
	Build() (string, error)
	ValidateBuilder() error
}

func contains(selectColumn []string, group string) bool {
	result := false
	for _, column := range selectColumn {
		if column == group {
			result = true
		}
	}
	return result
}

type metricSqlBuilder struct {
	selectColumns []string
	from          string
	where         []string
	groups        []string
	interval      int
	metricName    string
	start         time.Time
	end           time.Time
	sqlTemplate   string
}

func NewSumMetricSQLBuilder() SQLBuilder {
	return &metricSqlBuilder{sqlTemplate: string(sumSQLTemplate())}
}

func NewGaugeMetricSQLBuilder() SQLBuilder {
	return &metricSqlBuilder{sqlTemplate: string(gageSQLTemplate())}
}

func (b *metricSqlBuilder) Select(columns ...string) SQLBuilder {
	b.selectColumns = append(b.selectColumns, columns...)
	return b
}

// From sets the FROM table for the SQL statement.
func (b *metricSqlBuilder) From(table string) SQLBuilder {
	b.from = table
	return b
}

func (b *metricSqlBuilder) MetricName(name string) SQLBuilder {
	b.metricName = name
	return b
}

func (b *metricSqlBuilder) GetMetricName() string {
	return b.metricName
}

// Where adds a WHERE condition to the SQL statement.
func (b *metricSqlBuilder) Where(condition ...string) SQLBuilder {
	b.where = append(b.where, condition...)
	return b
}

func (b *metricSqlBuilder) Range(start, end time.Time) SQLBuilder {
	b.start = start
	b.end = end
	return b
}

func (b *metricSqlBuilder) Group(groups ...string) SQLBuilder {
	b.groups = append(b.groups, groups...)
	return b
}

// Interval sets the granularity interval for the SQL statement.
func (b *metricSqlBuilder) Interval(interval int) SQLBuilder {
	b.interval = interval
	return b
}

func (b *metricSqlBuilder) Build() (string, error) {

	err := b.ValidateBuilder()
	if err != nil {
		return "", err
	}

	funcs := template.FuncMap{
		"add": func(x int) int {
			return x + 1
		},
		"sub": func(a, b int) int {
			return a - b
		},
		"formatTime": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05")
		},
	}

	t := template.Must(template.New("sum-sql").Funcs(funcs).Parse(string(b.sqlTemplate)))

	bytes := bytes.Buffer{}
	t.Execute(&bytes, map[string]interface{}{
		"selectColumns": b.selectColumns,
		"where":         b.where,
		"from":          b.from,
		"groups":        b.groups,
		"interval":      b.interval,
		"metricName":    b.metricName,
		"start":         b.start,
		"end":           b.end,
	})

	result := bytes.String()

	var dangerousStatements = regexp.MustCompile(`(?i)(CREATE|INSERT|UPDATE|TRUNCATE|DROP|DELETE|;)\s`)

	if dangerousStatements.MatchString(result) {
		return "", fmt.Errorf("SQL statement contains dangerous keywords")
	}

	return bytes.String(), nil
}

func (b *metricSqlBuilder) ValidateBuilder() error {
	if b.from == "" {
		return fmt.Errorf("FROM table is required")
	}
	if len(b.selectColumns) == 0 {
		return fmt.Errorf("SELECT columns are required")
	}
	if b.metricName == "" {
		return fmt.Errorf("Metric name is required")
	}
	if b.interval == 0 {
		return fmt.Errorf("Interval is required")
	}
	if b.interval < 60 {
		return fmt.Errorf("Interval must be at least 60 seconds")
	}
	if b.start.IsZero() {
		return fmt.Errorf("start time is required")
	}
	if b.end.IsZero() {
		return fmt.Errorf("end time is required")
	}
	if b.end.Before(b.start) {
		return fmt.Errorf("Range invalid, 'end' cannot be less than 'start'")

	}

	if len(b.groups) > 0 {
		for _, group := range b.groups {
			if group == "" {
				return fmt.Errorf("Group column name can not be empty")
			}
			if !contains(b.selectColumns, group) {
				return fmt.Errorf("Group column %s is not in SELECT columns", group)
			}
		}
	}
	return nil
}

func sumSQLTemplate() string {
	return `{{ $grpLength := len .groups }}
{{ $length := len .selectColumns }}

{{ if gt $grpLength 0 }}
SELECT {{ range .groups }}{{ . }},{{ end }}
UsageTime, sum(Usage) Usage
FROM ( {{end}}

SELECT {{ range $index, $column := .selectColumns }} arrayElement(splitByString(':', increaseKey), {{ add $index }}) AS {{ $column}},{{ end }}
  toDateTime(intDiv(toUInt32(TimeUnix), {{ .interval }}) * {{ .interval }}) AS UsageTime,
  sum(IncreaseValue) as Usage
FROM (
    SELECT concat({{ range $index, $column := .selectColumns }}Attributes['{{ $column }}'] {{ if lt $index (sub $length 1) }},':', {{ end }} {{ end }}) as increaseKey,
    TimeUnix,
	MetricName,
    lagInFrame(Value) OVER (PARTITION BY increaseKey ORDER BY TimeUnix ASC ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS prevValue,
	0 Mark,
	COUNT(Mark) OVER (PARTITION BY increaseKey ORDER BY	TimeUnix ROWS 1 PRECEDING)-1 = 1 PrevExists,
	if(PrevExists,
	    if( prevValue > Value,
			if(prevValue = 0,
			    0,
			    Value),
		Value - prevValue),
	0) as IncreaseValue
    FROM {{ .from }}
    WHERE MetricName = '{{ .metricName }}'
	    AND NOT isNaN(Value)
        {{ range .where }} {{ . }} {{ end }}
        AND TimeUnix BETWEEN (toDateTime('{{ formatTime .start}}') - INTERVAL 300 SECOND) AND toDateTime('{{ formatTime .end}}') ) AS data
GROUP BY
	increaseKey,
	UsageTime
ORDER BY
	increaseKey,
	UsageTime

{{ if gt $grpLength 0 }}
) as grouped 
GROUP BY UsageTime,
    {{ range $index, $column := .groups }}
        {{ $column }}{{ if lt $index (sub $grpLength 1) }},{{ end }}{{ end }}  
ORDER BY {{ range .groups }}{{ . }},{{ end }}
UsageTime{{ end }}`

}

func gageSQLTemplate() string {
	return `{{ $grpLength := len .groups }}
{{ $length := len .selectColumns }}

{{ if gt $grpLength 0 }}
SELECT {{ range .groups }}{{ . }},{{ end }}
UsageTime, sum(Usage) Usage
FROM ( {{end}}
SELECT {{ range .selectColumns }}Attributes['{{ . }}'] as {{ . }}, {{ end }}
toDateTime(intDiv(toUInt32(TimeUnix), {{ .interval }}) * {{ .interval }}) AS UsageTime,
avg(Value)/1e6 as Usage
FROM {{ .from }}
WHERE MetricName = '{{ .metricName }}'
	AND NOT isNaN(Value)
    {{ range .where }} {{ . }} {{ end }}
    AND TimeUnix BETWEEN (toDateTime('{{ formatTime .start}}') - INTERVAL 300 SECOND) AND toDateTime('{{ formatTime .end}}')
GROUP BY UsageTime, {{ range $index, $column := .selectColumns }}{{ $column }}{{ if lt $index (sub $length 1) }},{{ end }}{{ end }}
ORDER BY UsageTime

{{ if gt $grpLength 0 }}
) as grouped 
GROUP BY UsageTime,
    {{ range $index, $column := .groups }}
        {{ $column }}{{ if lt $index (sub $grpLength 1) }},{{ end }}{{ end }}  
ORDER BY {{ range .groups }}{{ . }},{{ end }}
UsageTime{{ end }}`
}
