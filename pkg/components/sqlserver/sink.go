package sqlserver

import (
	"context"
	"database/sql"
	"datapipeline/pkg/pipeline"
	"fmt"
	"strings"

	mssql "github.com/denisenkom/go-mssqldb"
)

type FieldMapping struct {
	Source string
	Target string
}

type SQLServerSink struct {
	db     *sql.DB
	table  string
	fields []FieldMapping
}

func NewSQLServerSink(dsn string, table string, fields []FieldMapping) (*SQLServerSink, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &SQLServerSink{
		db:     db,
		table:  table,
		fields: fields,
	}, nil
}

func (s *SQLServerSink) Write(ctx context.Context, msg pipeline.Message) error {
	return s.WriteBatch(ctx, []pipeline.Message{msg})
}

func (s *SQLServerSink) WriteBatch(ctx context.Context, msgs []pipeline.Message) (err error) {
	if len(msgs) == 0 {
		return nil
	}
	if len(s.fields) == 0 {
		return fmt.Errorf("no mapped fields found")
	}

	// Build column list
	cols := make([]string, len(s.fields))
	for i, f := range s.fields {
		cols[i] = f.Target
	}

	// Begin a transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil { // This 'err' is the named return parameter
			_ = tx.Rollback()
		}
	}()

	// Prepare bulk copy statement using CopyIn
	stmt, err := tx.Prepare(mssql.CopyIn(s.table, mssql.BulkOptions{}, cols...))
	if err != nil {
		return err
	}
	defer func() {
		closeErr := stmt.Close()
		if err == nil && closeErr != nil { // Only set err if it's not already set
			err = closeErr
		}
	}()

	// Insert rows
	for _, msg := range msgs {
		row := make([]interface{}, len(s.fields))
		for i, field := range s.fields {
			row[i] = getValueFromMap(msg.Data, field.Source)
		}
		if _, err = stmt.Exec(row...); err != nil {
			return err
		}
	}
	// Finalize bulk copy
	if _, err = stmt.Exec(); err != nil {
		return err
	}
	// Commit transaction
	return tx.Commit()
}

func getValueFromMap(data map[string]interface{}, path string) interface{} {
	keys := strings.Split(path, ".")
	var current interface{} = data

	for _, key := range keys {
		if m, ok := current.(map[string]interface{}); ok {
			if val, exists := m[key]; exists {
				current = val
			} else {
				return nil
			}
		} else {
			return nil
		}
	}
	return current
}

func (s *SQLServerSink) Close() error {
	return s.db.Close()
}
