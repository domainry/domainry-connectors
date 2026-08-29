package databasesql

import (
	"context"
	"errors"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
)

// Client owns the transport-level SQL mechanics shared by external database
// Providers. Driver selection remains explicit in each Provider constructor;
// Client never branches on a driver name.
type Client struct {
	transport connector.Transport
	driver    string
	errorKey  string
}

func NewClient(transport connector.Transport, driver, errorKey string) (*Client, error) {
	if transport == nil || strings.TrimSpace(driver) == "" || strings.TrimSpace(errorKey) == "" {
		return nil, errors.New("database SQL client requires transport, driver, and error key")
	}
	return &Client{transport: transport, driver: driver, errorKey: errorKey}, nil
}

func (c *Client) Ping(ctx context.Context, dsn string, timeout time.Duration) error {
	_, err := c.transport.ExecuteSQL(ctx, connector.SQLRequest{Driver: c.driver, DSN: dsn, Operation: connector.SQLOperationPing, Timeout: timeout})
	if err != nil {
		return connector.RetryableError(c.errorKey+".ping_failed", err)
	}
	return nil
}

func (c *Client) Query(ctx context.Context, dsn, statement string, arguments []any, maxRows int, timeout time.Duration, failure string) (connector.SQLResult, error) {
	result, err := c.transport.ExecuteSQL(ctx, connector.SQLRequest{Driver: c.driver, DSN: dsn, Operation: connector.SQLOperationQuery, Statement: statement, Arguments: arguments, MaxRows: maxRows, Timeout: timeout})
	if err != nil {
		return connector.SQLResult{}, connector.RetryableError(c.errorKey+"."+failure, err)
	}
	return result, nil
}

func ProjectRows(result connector.SQLResult) []any {
	rows := make([]any, 0, len(result.Rows))
	for _, values := range result.Rows {
		item := map[string]any{}
		for index, column := range result.Columns {
			if index < len(values) {
				item[column] = StringValue(values[index])
			}
		}
		rows = append(rows, item)
	}
	return rows
}

func StringValue(value any) any {
	if raw, ok := value.([]byte); ok {
		return string(raw)
	}
	return value
}

func Text(value any) string {
	if raw, ok := StringValue(value).(string); ok {
		return raw
	}
	return ""
}
