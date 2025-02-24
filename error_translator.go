package duckdb

import (
	"gorm.io/gorm"
)

// DuckDB のエラーコードをGORMのエラーにマッピング
var errCodes = map[string]error{
	"23505": gorm.ErrDuplicatedKey,
	"23503": gorm.ErrForeignKeyViolated,
	"42703": gorm.ErrInvalidField,
}

// Translate はエラーをGORMネイティブのエラーに変換します
func (dialector Dialector) Translate(err error) error {
	// DuckDBのエラーは現時点では単純なエラー文字列として扱う
	// より詳細なエラーハンドリングが必要な場合は、
	// DuckDBのエラー型に応じて適切に処理を追加する
	return err
}
