package duckdb

import (
	"database/sql"
	"regexp"

	_ "github.com/marcboeker/go-duckdb" // DuckDB ドライバーを登録
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Dialector struct {
	*Config
}

type Config struct {
	DriverName string
	DSN        string
	Conn       gorm.ConnPool
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{&Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Name() string {
	return "duckdb"
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES"},
		UpdateClauses: []string{"UPDATE", "SET", "WHERE"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE"},
	})

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		db.ConnPool, err = sql.Open("duckdb", dialector.Config.DSN)
		if err != nil {
			return err
		}
	}

	return nil
}

func (dialector Dialector) Apply(config *gorm.Config) error {
	if config.NamingStrategy == nil {
		config.NamingStrategy = schema.NamingStrategy{}
	}
	return nil
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   dialector,
		CreateIndexAfterCreateTable: true,
	}}}
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	writer.WriteString(str)
	writer.WriteByte('"')
}

var numericPlaceholder = regexp.MustCompile(`\?`)

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, numericPlaceholder, `'`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "BOOLEAN"
	case schema.Int, schema.Uint:
		return "INTEGER"
	case schema.Float:
		return "DOUBLE"
	case schema.String:
		return "VARCHAR"
	case schema.Time:
		return "TIMESTAMP"
	case schema.Bytes:
		return "BLOB"
	}
	return string(field.DataType)
}

func (dialector Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVEPOINT " + name)
	return nil
}

func (dialector Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	return nil
}
