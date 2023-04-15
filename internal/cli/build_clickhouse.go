//go:build clickhouse
// +build clickhouse

package cli

import (
	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/dendy1/migrate/v4/database/clickhouse"
)
