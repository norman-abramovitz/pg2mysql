package pg2mysql

import (
	"database/sql"
	"errors"
	"fmt"
    "log"
	"os"
	"strings"
)

type Migrator interface {
	Migrate() error
}

func NewMigrator(src, dst DB, truncateFirst bool, watcher MigratorWatcher, debug map[string]bool) Migrator {
	return &migrator{
		src:           src,
		dst:           dst,
		truncateFirst: truncateFirst,
		watcher:       watcher,
        debug:         debug,
	}
}

type migrator struct {
	src, dst      DB
	truncateFirst bool
	watcher       MigratorWatcher
    debug         map[string]bool
}

func (m *migrator) Migrate() error {
	srcSchema, err := BuildSchema(m.src)
	if err != nil {
		return fmt.Errorf("failed to build source schema: %s", err)
	}

	dstSchema, err := BuildSchema(m.dst)
	if err != nil {
		return fmt.Errorf("failed to build destination schema: %s", err)
	}

	m.watcher.WillDisableConstraints()
	err = m.dst.DisableConstraints()
	if err != nil {
		return fmt.Errorf("failed to disable constraints: %s", err)
	}
	m.watcher.DidDisableConstraints()

	defer func() {
		m.watcher.WillEnableConstraints()
		err = m.dst.EnableConstraints()
		if err != nil {
			m.watcher.EnableConstraintsDidFailWithError(err)
		} else {
			m.watcher.EnableConstraintsDidFinish()
		}
	}()

	for _, table := range srcSchema.Tables {
		dstTable, err := dstSchema.GetTable(table.NormalizedName)
		if err != nil {
            return fmt.Errorf("failed to get table from destination schema: %s", err)
        }
		if m.truncateFirst {
			m.watcher.WillTruncateTable(dstTable.ActualName)
            stmt := fmt.Sprintf("TRUNCATE TABLE %s", dstTable.ActualName)

            if m.debug["sql"] {
                fmt.Println("DEBUG SQL:", stmt)
            }

			_, err := m.dst.DB().Exec(stmt)
			if err != nil {
				return fmt.Errorf("failed truncating: %s", err)
			}
			m.watcher.TruncateTableDidFinish(table.ActualName)
		}

		columnNamesForInsert := make([]string, len(dstTable.Columns))
		placeholders := make([]string, len(dstTable.Columns))
		for i := range table.Columns {
			columnNamesForInsert[i] = m.dst.ColumnNameForSelect(dstTable.Columns[i].ActualName)
            if table.Columns[i].Type == "uuid" {
                placeholders[i] = "unhex(replace(" + m.dst.ParameterMarker(i) + ",'-',''))"
            } else {
			    placeholders[i] = m.dst.ParameterMarker(i)
            }
		}


        stmt := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			dstTable.ActualName,
			strings.Join(columnNamesForInsert, ","),
			strings.Join(placeholders, ","),
		)

		preparedStmt, err := m.dst.DB().Prepare(stmt)

        // omgdebug
        if m.debug["sql"] {
            fmt.Println("DEBUG SQL:", stmt)
        }

		if err != nil {
			return fmt.Errorf("failed creating prepared statement: %s", err)
		}

		var recordsInserted int64

		m.watcher.TableMigrationDidStart(table.ActualName)

		if table.HasColumn(&IDColumn) {
			err = migrateWithIDs(m.watcher, m.src, m.dst, table, dstTable, m.debug, &recordsInserted, preparedStmt)
			if err != nil {
				return fmt.Errorf("failed migrating table with ids: %s", err)
			}
		} else {
			err = EachMissingRow(m.src, m.dst, table, dstTable, m.debug, func(scanArgs []interface{}) {
				err = insert(preparedStmt, scanArgs)
				if err != nil {
                    fmt.Fprintf(os.Stderr,  "%s\n", preparedStmt  );
					fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.ActualName, err)
					return
				}
				recordsInserted++
                if recordsInserted == 1 || recordsInserted % 10 == 0 {
                    m.watcher.TableMigrationInProgress( table.ActualName, recordsInserted)
                }
			})
			if err != nil {
				return fmt.Errorf("failed migrating table without ids: %s", err)
			}
		}

		m.watcher.TableMigrationDidFinish(table.ActualName, recordsInserted)
	}

	return nil
}

func migrateWithIDs(
	watcher MigratorWatcher,
	src DB,
	dst DB,
	table *Table,
	dstTable *Table,
    debug map[string]bool,
	recordsInserted *int64,
	preparedStmt *sql.Stmt,
) error {
	columnNamesForSelect := make([]string, len(table.Columns))
	values := make([]interface{}, len(table.Columns))
	scanArgs := make([]interface{}, len(table.Columns))
	for i := range table.Columns {
		columnNamesForSelect[i] = table.Columns[i].ActualName
		scanArgs[i] = &values[i]
	}

	// find ids already in dst
    stmt := fmt.Sprintf("SELECT id FROM %s", table.ActualName)
    if debug["sql"] {
        fmt.Println("DEBUG SQL:", stmt)
    }
	rows, err := dst.DB().Query(stmt)
	if err != nil {
		return fmt.Errorf("failed to select id from rows: %s", err)
	}

	var dstIDs []interface{}
	for rows.Next() {
		var id interface{}
		if err = rows.Scan(&id); err != nil {
			return fmt.Errorf("failed to scan id from row: %s", err)
		}
		dstIDs = append(dstIDs, id)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("failed closing rows: %s", err)
	}

	// select data for ids to migrate from src
	stmt = fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(columnNamesForSelect, ","),
		table.ActualName,
	)
	selectArgs := make([]interface{}, 0)

	if len(dstIDs) > 0 && len(dstIDs) < 65535 {
		placeholders := make([]string, len(dstIDs))
		for i := range dstIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		stmt = fmt.Sprintf("%s WHERE id NOT IN (%s)", stmt, strings.Join(placeholders, ","))
		selectArgs = dstIDs
	}

    if debug["sql"] {
        fmt.Println("DEBUG SQL:", stmt)
    }
	rows, err = src.DB().Query(stmt, selectArgs...)
	if err != nil {
		return fmt.Errorf("failed to select rows: %s", err)
	}

	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan row: %s", err)
		}
        if debug["data"] {
            for i := range scanArgs {
                arg := scanArgs[i]
                iface, ok := arg.(*interface{})
                if !ok {
                    log.Fatalf("received unexpected type as scanArg: %T (should be *interface{})", arg)
                }
                fmt.Printf( "DEBUG scanArgs : %d:  %T  %v  ", i, *iface, *iface )
            }
            fmt.Printf("\n")
        }
        if debug["sql"] {
            fmt.Println("DEBUG SQL: ", preparedStmt)
        }

		err = insert(preparedStmt, scanArgs)
		if err != nil {
			if !isPrimaryKeyError(err) {
				fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.ActualName, err)
			}
			continue
		}

		*recordsInserted++
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("failed closing rows: %s", err)
	}

	return nil
}

func insert(stmt *sql.Stmt, values []interface{}) error {
	result, err := stmt.Exec(values...)
	if err != nil {
		return fmt.Errorf("failed to exec stmt: %s", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed getting rows affected by insert: %s", err)
	}

	if rowsAffected == 0 {
		return errors.New("no rows affected by insert")
	}

	return nil
}

func isPrimaryKeyError(err error) bool {
	return strings.Contains(err.Error(), "pkey") && strings.Contains(err.Error(), "duplicate key value violates unique constraint")
}
