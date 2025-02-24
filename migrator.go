package duckdb

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// See https://stackoverflow.com/questions/2204058/list-columns-with-indexes-in-postgresql
// Here are some changes:
// - use `LEFT JOIN` instead of `CROSS JOIN`
// - exclude indexes used to support constraints (they are auto-generated)
// DuckDBではインデックス情報を取得するためのシステムテーブルが異なります
const indexSql = `
SELECT
    table_name,
    index_name,
    0 as non_unique,
    0 as primary,
    column_name
FROM pragma_index_info(?)
`

var typeAliasMap = map[string][]string{
	"int":      {"integer"},
	"integer":  {"int"},
	"bool":     {"boolean"},
	"boolean":  {"bool"},
	"varchar":  {"string", "text"},
	"double":   {"float", "real"},
	"blob":     {"binary"},
	"datetime": {"timestamp"},
}

type Migrator struct {
	migrator.Migrator
}

// select querys ignore dryrun
func (m Migrator) queryRaw(sql string, values ...interface{}) (tx *gorm.DB) {
	queryTx := m.DB
	if m.DB.DryRun {
		queryTx = m.DB.Session(&gorm.Session{})
		queryTx.DryRun = false
	}
	return queryTx.Raw(sql, values...)
}

func (m Migrator) CurrentDatabase() (name string) {
	m.queryRaw("SELECT CURRENT_DATABASE()").Scan(&name)
	return
}

func (m Migrator) BuildIndexOptions(opts []schema.IndexOption, stmt *gorm.Statement) (results []interface{}) {
	for _, opt := range opts {
		str := stmt.Quote(opt.DBName)
		if opt.Expression != "" {
			str = opt.Expression
		}

		if opt.Collate != "" {
			str += " COLLATE " + opt.Collate
		}

		if opt.Sort != "" {
			str += " " + opt.Sort
		}
		results = append(results, clause.Expr{SQL: str})
	}
	return
}

func (m Migrator) HasIndex(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if idx := stmt.Schema.LookIndex(name); idx != nil {
				name = idx.Name
			}
		}
		return m.queryRaw(
			"SELECT COUNT(*) FROM pragma_index_list(?) WHERE name = ?",
			m.CurrentTable(stmt), name,
		).Scan(&count).Error
	})

	return count > 0
}

func (m Migrator) CreateIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if idx := stmt.Schema.LookIndex(name); idx != nil {
				opts := m.BuildIndexOptions(idx.Fields, stmt)
				values := []interface{}{clause.Column{Name: idx.Name}, m.CurrentTable(stmt), opts}

				createIndexSQL := "CREATE "
				if idx.Class != "" {
					createIndexSQL += idx.Class + " "
				}
				createIndexSQL += "INDEX "

				if strings.TrimSpace(strings.ToUpper(idx.Option)) == "CONCURRENTLY" {
					createIndexSQL += "CONCURRENTLY "
				}

				createIndexSQL += "IF NOT EXISTS ? ON ?"

				if idx.Type != "" {
					createIndexSQL += " USING " + idx.Type + "(?)"
				} else {
					createIndexSQL += " ?"
				}

				if idx.Option != "" && strings.TrimSpace(strings.ToUpper(idx.Option)) != "CONCURRENTLY" {
					createIndexSQL += " " + idx.Option
				}

				if idx.Where != "" {
					createIndexSQL += " WHERE " + idx.Where
				}

				return m.DB.Exec(createIndexSQL, values...).Error
			}
		}

		return fmt.Errorf("failed to create index with name %v", name)
	})
}

func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Exec(
			"ALTER INDEX ? RENAME TO ?",
			clause.Column{Name: oldName}, clause.Column{Name: newName},
		).Error
	})
}

func (m Migrator) DropIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if idx := stmt.Schema.LookIndex(name); idx != nil {
				name = idx.Name
			}
		}

		return m.DB.Exec("DROP INDEX ?", clause.Column{Name: name}).Error
	})
}

func (m Migrator) GetTables() (tableList []string, err error) {
	return tableList, m.queryRaw("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tableList).Error
}

func (m Migrator) CreateTable(values ...interface{}) (err error) {
	// First create tables without sequences
	if err = m.Migrator.CreateTable(values...); err != nil {
		return
	}

	// Then add sequences and update default values
	for _, value := range m.ReorderModels(values, false) {
		if err = m.RunWithValue(value, func(stmt *gorm.Statement) error {
			if stmt.Schema != nil {
				for _, field := range stmt.Schema.Fields {
					if field.Name == "ID" && field.AutoIncrement {
						tableName := stmt.Table
						seqName := tableName + "_seq"
						
						// Create sequence
						if err := m.DB.Exec("CREATE SEQUENCE IF NOT EXISTS " + seqName + " START 1").Error; err != nil {
							return err
						}

						// Alter column to use sequence
						if err := m.DB.Exec("ALTER TABLE " + tableName + " ALTER COLUMN " + field.DBName + " SET DEFAULT nextval('" + seqName + "')").Error; err != nil {
							return err
						}
					}
				}
			}
			return nil
		}); err != nil {
			return
		}
	}

	return nil
}

func (m Migrator) HasTable(value interface{}) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		currentDatabase := m.DB.Migrator().CurrentDatabase()
		return m.DB.Raw(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			currentDatabase, stmt.Table,
		).Scan(&count).Error
	})

	return count > 0
}

func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)
	tx := m.DB.Session(&gorm.Session{})
	for i := len(values) - 1; i >= 0; i-- {
		if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
			return tx.Exec("DROP TABLE IF EXISTS ? CASCADE", m.CurrentTable(stmt)).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m Migrator) AddColumn(value interface{}, field string) error {
	if err := m.Migrator.AddColumn(value, field); err != nil {
		return err
	}
	m.resetPreparedStmts()

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if field := stmt.Schema.LookUpField(field); field != nil {
				if field.Comment != "" {
					if err := m.DB.Exec(
						"COMMENT ON COLUMN ?.? IS ?",
						m.CurrentTable(stmt), clause.Column{Name: field.DBName}, gorm.Expr(m.Migrator.Dialector.Explain("$1", field.Comment)),
					).Error; err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

func (m Migrator) HasColumn(value interface{}, field string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		name := field
		if stmt.Schema != nil {
			if field := stmt.Schema.LookUpField(field); field != nil {
				name = field.DBName
			}
		}

		return m.queryRaw(
			"SELECT count(*) FROM pragma_table_info(?) WHERE name = ?",
			m.CurrentTable(stmt), name,
		).Scan(&count).Error
	})

	return count > 0
}

func (m Migrator) MigrateColumn(value interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	if !field.PrimaryKey {
		if err := m.Migrator.MigrateColumn(value, field, columnType); err != nil {
			return err
		}
	}
	// DuckDBではコメントはサポートされていないため、コメント関連の処理は不要
	return nil
}

// AlterColumn alter value's `field` column' type based on schema definition
func (m Migrator) AlterColumn(value interface{}, field string) error {
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if field := stmt.Schema.LookUpField(field); field != nil {
				var (
					columnTypes, _  = m.DB.Migrator().ColumnTypes(value)
					fieldColumnType *migrator.ColumnType
				)
				for _, columnType := range columnTypes {
					if columnType.Name() == field.DBName {
						fieldColumnType, _ = columnType.(*migrator.ColumnType)
					}
				}

				fileType := clause.Expr{SQL: m.DataTypeOf(field)}
				// check for typeName and SQL name
				isSameType := true
				if !strings.EqualFold(fieldColumnType.DatabaseTypeName(), fileType.SQL) {
					isSameType = false
					// if different, also check for aliases
					aliases := m.GetTypeAliases(fieldColumnType.DatabaseTypeName())
					for _, alias := range aliases {
						if strings.HasPrefix(fileType.SQL, alias) {
							isSameType = true
							break
						}
					}
				}

				// not same, migrate
				if !isSameType {
					filedColumnAutoIncrement, _ := fieldColumnType.AutoIncrement()
					if field.AutoIncrement && filedColumnAutoIncrement { // update
						serialDatabaseType, _ := getSerialDatabaseType(fileType.SQL)
						if t, _ := fieldColumnType.ColumnType(); t != serialDatabaseType {
							if err := m.UpdateSequence(m.DB, stmt, field, serialDatabaseType); err != nil {
								return err
							}
						}
					} else if field.AutoIncrement && !filedColumnAutoIncrement { // create
						serialDatabaseType, _ := getSerialDatabaseType(fileType.SQL)
						if err := m.CreateSequence(m.DB, stmt, field, serialDatabaseType); err != nil {
							return err
						}
					} else if !field.AutoIncrement && filedColumnAutoIncrement { // delete
						if err := m.DeleteSequence(m.DB, stmt, field, fileType); err != nil {
							return err
						}
					} else {
						if err := m.modifyColumn(stmt, field, fileType, fieldColumnType); err != nil {
							return err
						}
					}
				}

				if null, _ := fieldColumnType.Nullable(); null == field.NotNull {
					if field.NotNull {
						if err := m.DB.Exec("ALTER TABLE ? ALTER COLUMN ? SET NOT NULL", m.CurrentTable(stmt), clause.Column{Name: field.DBName}).Error; err != nil {
							return err
						}
					} else {
						if err := m.DB.Exec("ALTER TABLE ? ALTER COLUMN ? DROP NOT NULL", m.CurrentTable(stmt), clause.Column{Name: field.DBName}).Error; err != nil {
							return err
						}
					}
				}

				if v, ok := fieldColumnType.DefaultValue(); (field.DefaultValueInterface == nil && ok) || v != field.DefaultValue {
					if field.HasDefaultValue && (field.DefaultValueInterface != nil || field.DefaultValue != "") {
						if field.DefaultValueInterface != nil {
							defaultStmt := &gorm.Statement{Vars: []interface{}{field.DefaultValueInterface}}
							m.Dialector.BindVarTo(defaultStmt, defaultStmt, field.DefaultValueInterface)
							if err := m.DB.Exec(
								"ALTER TABLE ? ALTER COLUMN ? SET DEFAULT ?",
								m.CurrentTable(stmt), clause.Column{Name: field.DBName}, clause.Expr{SQL: m.Dialector.Explain(defaultStmt.SQL.String(), field.DefaultValueInterface)},
							).Error; err != nil {
								return err
							}
						} else if field.DefaultValue != "(-)" {
							if err := m.DB.Exec("ALTER TABLE ? ALTER COLUMN ? SET DEFAULT ?", m.CurrentTable(stmt), clause.Column{Name: field.DBName}, clause.Expr{SQL: field.DefaultValue}).Error; err != nil {
								return err
							}
						} else {
							if err := m.DB.Exec("ALTER TABLE ? ALTER COLUMN ? DROP DEFAULT", m.CurrentTable(stmt), clause.Column{Name: field.DBName}).Error; err != nil {
								return err
							}
						}
					} else if !field.HasDefaultValue {
						// case - as-is column has default value and to-be column has no default value
						// need to drop default
						if err := m.DB.Exec("ALTER TABLE ? ALTER COLUMN ? DROP DEFAULT", m.CurrentTable(stmt), clause.Column{Name: field.DBName}).Error; err != nil {
							return err
						}
					}
				}
				return nil
			}
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})

	if err != nil {
		return err
	}
	m.resetPreparedStmts()
	return nil
}

func (m Migrator) modifyColumn(stmt *gorm.Statement, field *schema.Field, targetType clause.Expr, existingColumn *migrator.ColumnType) error {
	alterSQL := "ALTER TABLE ? ALTER COLUMN ? TYPE ? USING ?::?"
	isUncastableDefaultValue := false

	if targetType.SQL == "boolean" {
		switch existingColumn.DatabaseTypeName() {
		case "int2", "int8", "numeric":
			alterSQL = "ALTER TABLE ? ALTER COLUMN ? TYPE ? USING ?::int::?"
		}
		isUncastableDefaultValue = true
	}

	if dv, _ := existingColumn.DefaultValue(); dv != "" && isUncastableDefaultValue {
		if err := m.DB.Exec("ALTER TABLE ? ALTER COLUMN ? DROP DEFAULT", m.CurrentTable(stmt), clause.Column{Name: field.DBName}).Error; err != nil {
			return err
		}
	}
	if err := m.DB.Exec(alterSQL, m.CurrentTable(stmt), clause.Column{Name: field.DBName}, targetType, clause.Column{Name: field.DBName}, targetType).Error; err != nil {
		return err
	}
	return nil
}

func (m Migrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, table := m.GuessConstraintInterfaceAndTable(stmt, name)
		if constraint != nil {
			name = constraint.GetName()
		}
		currentSchema, curTable := m.CurrentSchema(stmt, table)

		return m.queryRaw(
			"SELECT count(*) FROM INFORMATION_SCHEMA.table_constraints WHERE table_schema = ? AND table_name = ? AND constraint_name = ?",
			currentSchema, curTable, name,
		).Scan(&count).Error
	})

	return count > 0
}

func (m Migrator) ColumnTypes(value interface{}) (columnTypes []gorm.ColumnType, err error) {
	columnTypes = make([]gorm.ColumnType, 0)
	err = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var columns *sql.Rows
		columns, err = m.queryRaw(
			"SELECT name, type, notnull, dflt_value FROM pragma_table_info(?)",
			m.CurrentTable(stmt)).Rows()

		if err != nil {
			return err
		}

		for columns.Next() {
			var (
				name         string
				typeName     string
				notNull      bool
				defaultValue sql.NullString
			)

			if err := columns.Scan(&name, &typeName, &notNull, &defaultValue); err != nil {
				return err
			}

			column := &migrator.ColumnType{
				NameValue:         sql.NullString{String: name, Valid: true},
				DataTypeValue:     sql.NullString{String: typeName, Valid: true},
				ColumnTypeValue:   sql.NullString{String: typeName, Valid: true},
				NullableValue:     sql.NullBool{Bool: !notNull, Valid: true},
				DefaultValueValue: defaultValue,
				PrimaryKeyValue:   sql.NullBool{Valid: true},
				UniqueValue:       sql.NullBool{Valid: true},
			}

			columnTypes = append(columnTypes, column)
		}
		columns.Close()

		// Get primary key and unique constraints
		pkRows, err := m.queryRaw("SELECT name FROM pragma_table_info(?) WHERE pk > 0", m.CurrentTable(stmt)).Rows()
		if err != nil {
			return err
		}
		for pkRows.Next() {
			var name string
			if err := pkRows.Scan(&name); err != nil {
				return err
			}
			for _, c := range columnTypes {
				mc := c.(*migrator.ColumnType)
				if mc.NameValue.String == name {
					mc.PrimaryKeyValue = sql.NullBool{Bool: true, Valid: true}
					break
				}
			}
		}
		pkRows.Close()

		// assign sql column type using current connection
		rows, err := m.DB.Session(&gorm.Session{}).Table(stmt.Table).Limit(1).Rows()
		if err != nil {
			return err
		}
		rawColumnTypes, err := rows.ColumnTypes()
		if err != nil {
			return err
		}
		for _, columnType := range columnTypes {
			for _, c := range rawColumnTypes {
				if c.Name() == columnType.Name() {
					columnType.(*migrator.ColumnType).SQLColumnType = c
					break
				}
			}
		}
		rows.Close()

		return nil
	})
	return
}

func (m Migrator) GetRows(currentSchema interface{}, table interface{}) (*sql.Rows, error) {
	name := table.(string)
	if _, ok := currentSchema.(string); ok {
		name = fmt.Sprintf("%v.%v", currentSchema, table)
	}

	return m.DB.Session(&gorm.Session{}).Table(name).Limit(1).Scopes(func(d *gorm.DB) *gorm.DB {
		return d
	}).Rows()
}

func (m Migrator) CurrentSchema(stmt *gorm.Statement, table string) (interface{}, interface{}) {
	if strings.Contains(table, ".") {
		if tables := strings.Split(table, `.`); len(tables) == 2 {
			return tables[0], tables[1]
		}
	}

	if stmt.TableExpr != nil {
		if tables := strings.Split(stmt.TableExpr.SQL, `"."`); len(tables) == 2 {
			return strings.TrimPrefix(tables[0], `"`), table
		}
	}
	return clause.Expr{SQL: "CURRENT_SCHEMA()"}, table
}

func (m Migrator) CreateSequence(tx *gorm.DB, stmt *gorm.Statement, field *schema.Field,
	serialDatabaseType string) (err error) {
	// DuckDBではAUTOINCREMENTを使用
	return tx.Exec("ALTER TABLE ? MODIFY COLUMN ? ? AUTOINCREMENT",
		m.CurrentTable(stmt), clause.Column{Name: field.DBName},
		clause.Expr{SQL: serialDatabaseType}).Error
}

func (m Migrator) UpdateSequence(tx *gorm.DB, stmt *gorm.Statement, field *schema.Field,
	serialDatabaseType string) (err error) {
	// DuckDBではAUTOINCREMENTの変更は新しいカラムの作成と古いカラムの削除が必要
	return tx.Exec("ALTER TABLE ? MODIFY COLUMN ? ? AUTOINCREMENT",
		m.CurrentTable(stmt), clause.Column{Name: field.DBName},
		clause.Expr{SQL: serialDatabaseType}).Error
}

func (m Migrator) DeleteSequence(tx *gorm.DB, stmt *gorm.Statement, field *schema.Field,
	fileType clause.Expr) (err error) {
	// DuckDBではAUTOINCREMENTを通常のカラムに変更
	return tx.Exec("ALTER TABLE ? MODIFY COLUMN ? ?",
		m.CurrentTable(stmt), clause.Column{Name: field.DBName}, fileType).Error
}

func (m Migrator) getColumnSequenceName(tx *gorm.DB, stmt *gorm.Statement, field *schema.Field) (
	sequenceName string, err error) {
	_, table := m.CurrentSchema(stmt, stmt.Table)

	// DefaultValueValue is reset by ColumnTypes, search again.
	var columnDefault string
	err = tx.Raw(
		`SELECT column_default FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
		table, field.DBName).Scan(&columnDefault).Error

	if err != nil {
		return
	}

	sequenceName = strings.TrimSuffix(
		strings.TrimPrefix(columnDefault, `nextval('`),
		`'::regclass)`,
	)
	return
}

func (m Migrator) GetIndexes(value interface{}) ([]gorm.Index, error) {
	indexes := make([]gorm.Index, 0)

	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		result := make([]*Index, 0)
		scanErr := m.queryRaw(indexSql, stmt.Table).Scan(&result).Error
		if scanErr != nil {
			return scanErr
		}
		indexMap := groupByIndexName(result)
		for _, idx := range indexMap {
			tempIdx := &migrator.Index{
				TableName: idx[0].TableName,
				NameValue: idx[0].IndexName,
				PrimaryKeyValue: sql.NullBool{
					Bool:  idx[0].Primary,
					Valid: true,
				},
				UniqueValue: sql.NullBool{
					Bool:  idx[0].NonUnique,
					Valid: true,
				},
			}
			for _, x := range idx {
				tempIdx.ColumnList = append(tempIdx.ColumnList, x.ColumnName)
			}
			indexes = append(indexes, tempIdx)
		}
		return nil
	})
	return indexes, err
}

// Index table index info
type Index struct {
	TableName  string `gorm:"column:table_name"`
	ColumnName string `gorm:"column:column_name"`
	IndexName  string `gorm:"column:index_name"`
	NonUnique  bool   `gorm:"column:non_unique"`
	Primary    bool   `gorm:"column:primary"`
}

func groupByIndexName(indexList []*Index) map[string][]*Index {
	columnIndexMap := make(map[string][]*Index, len(indexList))
	for _, idx := range indexList {
		columnIndexMap[idx.IndexName] = append(columnIndexMap[idx.IndexName], idx)
	}
	return columnIndexMap
}

func getSerialDatabaseType(columnType string) (string, error) {
	switch columnType {
	case "smallserial", "serial2":
		return "smallint", nil
	case "serial", "serial4":
		return "integer", nil
	case "bigserial", "serial8":
		return "bigint", nil
	}
	return "", fmt.Errorf("invalid serial type: %s", columnType)
}

func (m Migrator) GetTypeAliases(databaseTypeName string) []string {
	return typeAliasMap[databaseTypeName]
}

// should reset prepared stmts when table changed
func (m Migrator) resetPreparedStmts() {
	if m.DB.PrepareStmt {
		if pdb, ok := m.DB.ConnPool.(*gorm.PreparedStmtDB); ok {
			pdb.Reset()
		}
	}
}

func (m Migrator) DropColumn(dst interface{}, field string) error {
	if err := m.Migrator.DropColumn(dst, field); err != nil {
		return err
	}

	m.resetPreparedStmts()
	return nil
}

func (m Migrator) RenameColumn(dst interface{}, oldName, field string) error {
	if err := m.Migrator.RenameColumn(dst, oldName, field); err != nil {
		return err
	}

	m.resetPreparedStmts()
	return nil
}

func parseDefaultValueValue(defaultValue string) string {
	value := regexp.MustCompile(`^(.*?)(?:::.*)?$`).ReplaceAllString(defaultValue, "$1")
	return strings.Trim(value, "'")
}
