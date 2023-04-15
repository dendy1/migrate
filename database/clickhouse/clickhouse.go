package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.uber.org/atomic"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/hashicorp/go-multierror"
)

var (
	multiStmtDelimiter = []byte(";")

	DefaultMigrationsTable       = "schema_migrations"
	DefaultMigrationsTableEngine = "TinyLog"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB

	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	DatabaseName          string
	ClusterName           string
	MigrationsTable       string
	MigrationsTableEngine string
	MultiStatementEnabled bool
	MultiStatementMaxSize int
}

func init() {
	database.Register("clickhouse", &ClickHouse{})
}

func WithInstance(ctx context.Context, conn driver.Conn, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	ch := &ClickHouse{
		ctx:    context.Background(),
		conn:   conn,
		config: config,
	}

	if err := ch.init(); err != nil {
		return nil, err
	}

	return ch, nil
}

type ClickHouse struct {
	ctx      context.Context
	conn     driver.Conn
	config   *Config
	isLocked atomic.Bool
}

func (ch *ClickHouse) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	q := migrate.FilterCustomQuery(purl)
	q.Scheme = "tcp"

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{purl.Host},
		Auth: clickhouse.Auth{
			Database: purl.Query().Get("database"),
			Username: purl.Query().Get("username"),
			Password: purl.Query().Get("password"),
		},
	})

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := purl.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
	}

	migrationsTableEngine := DefaultMigrationsTableEngine
	if s := purl.Query().Get("x-migrations-table-engine"); len(s) > 0 {
		migrationsTableEngine = s
	}

	ch = &ClickHouse{
		ctx:  context.Background(),
		conn: conn,
		config: &Config{
			MigrationsTable:       purl.Query().Get("x-migrations-table"),
			MigrationsTableEngine: migrationsTableEngine,
			DatabaseName:          purl.Query().Get("database"),
			ClusterName:           purl.Query().Get("x-cluster-name"),
			MultiStatementEnabled: purl.Query().Get("x-multi-statement") == "true",
			MultiStatementMaxSize: multiStatementMaxSize,
		},
	}

	if err := ch.init(); err != nil {
		return nil, err
	}

	return ch, nil
}

func (ch *ClickHouse) init() error {
	if len(ch.config.DatabaseName) == 0 {
		if err := ch.conn.QueryRow(ch.ctx, "SELECT currentDatabase()").Scan(&ch.config.DatabaseName); err != nil {
			return err
		}
	}

	if len(ch.config.MigrationsTable) == 0 {
		ch.config.MigrationsTable = DefaultMigrationsTable
	}

	if ch.config.MultiStatementMaxSize <= 0 {
		ch.config.MultiStatementMaxSize = DefaultMultiStatementMaxSize
	}

	if len(ch.config.MigrationsTableEngine) == 0 {
		ch.config.MigrationsTableEngine = DefaultMigrationsTableEngine
	}

	return ch.ensureVersionTable()
}

func (ch *ClickHouse) Run(r io.Reader) error {
	if ch.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(r, multiStmtDelimiter, ch.config.MultiStatementMaxSize, func(m []byte) bool {
			tq := strings.TrimSpace(string(m))
			if tq == "" {
				return true
			}
			if e := ch.conn.Exec(ch.ctx, string(m)); e != nil {
				err = database.Error{OrigErr: e, Err: "migration failed", Query: m}
				return false
			}
			return true
		}); e != nil {
			return e
		}
		return err
	}

	migration, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if err := ch.conn.Exec(ch.ctx, string(migration)); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migration}
	}

	return nil
}
func (ch *ClickHouse) Version() (int, bool, error) {
	var (
		version int64
		dirty   uint8
		query   = "SELECT version, dirty FROM `" + ch.config.MigrationsTable + "` ORDER BY sequence DESC LIMIT 1"
	)
	if err := ch.conn.QueryRow(ch.ctx, query).Scan(&version, &dirty); err != nil {
		if err == sql.ErrNoRows {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return int(version), dirty == 1, nil
}

func (ch *ClickHouse) SetVersion(version int, dirty bool) error {
	query := "INSERT INTO " + ch.config.MigrationsTable + " (version, dirty, sequence) VALUES (?, ?, ?)"
	if err := ch.conn.Exec(ch.ctx, query, version, dirty, time.Now().UnixNano()); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the ClickHouse type.
func (ch *ClickHouse) ensureVersionTable() (err error) {
	if err = ch.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := ch.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	var (
		table string
		query = "SHOW TABLES FROM " + ch.config.DatabaseName + " LIKE '" + ch.config.MigrationsTable + "'"
	)
	// check if migration table exists
	if err := ch.conn.QueryRow(ch.ctx, query).Scan(&table); err != nil {
		if err != sql.ErrNoRows {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	} else {
		return nil
	}

	// if not, create the empty migration table
	if len(ch.config.ClusterName) > 0 {
		query = fmt.Sprintf(`
			CREATE TABLE %s ON CLUSTER %s (
				version    Int64,
				dirty      UInt8,
				sequence   UInt64
			) Engine=%s`, ch.config.MigrationsTable, ch.config.ClusterName, ch.config.MigrationsTableEngine)
	} else {
		query = fmt.Sprintf(`
			CREATE TABLE %s (
				version    Int64,
				dirty      UInt8,
				sequence   UInt64
			) Engine=%s`, ch.config.MigrationsTable, ch.config.MigrationsTableEngine)
	}

	if strings.HasSuffix(ch.config.MigrationsTableEngine, "Tree") {
		query = fmt.Sprintf(`%s ORDER BY sequence`, query)
	}

	if err := ch.conn.Exec(ch.ctx, query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (ch *ClickHouse) Drop() (err error) {
	query := "SHOW TABLES FROM " + ch.config.DatabaseName
	tables, err := ch.conn.Query(ch.ctx, query)

	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	for tables.Next() {
		var table string
		if err := tables.Scan(&table); err != nil {
			return err
		}

		query = "DROP TABLE IF EXISTS " + ch.config.DatabaseName + "." + table

		if err := ch.conn.Exec(ch.ctx, query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (ch *ClickHouse) Lock() error {
	if !ch.isLocked.CAS(false, true) {
		return database.ErrLocked
	}

	return nil
}
func (ch *ClickHouse) Unlock() error {
	if !ch.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}

	return nil
}
func (ch *ClickHouse) Close() error { return ch.conn.Close() }
