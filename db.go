package pg2mysql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

type DB interface {
	Open() error
	Close() error
	GetSchemaRows() (*sql.Rows, error)
	DisableConstraints() error
	EnableConstraints() error
	ColumnNameForSelect(columnName string) string
	ParameterMarker(paramIndex int) string
	DB() *sql.DB
	NormalizeTime(time.Time) time.Time
	ComparisonClause(paramIndex int, columnName string, columnType string) string
}

type Schema struct {
	Tables map[string]*Table
}

 
func (s *Schema) GetTable(normalizedName string) (*Table, error) {
	if table, ok := s.Tables[normalizedName]; ok {
		return table, nil
	}
	return nil, fmt.Errorf("table '%s' not found", normalizedName)
}

type Table struct {
	ActualName    string
	NormalizedName    string
	Columns []*Column
}

func (t *Table) HasColumn(other *Column) bool {
	_, _, err := t.GetColumn(other)
	return err == nil
}

func (t *Table) GetColumn(other *Column) (int, *Column, error) {
	for i, column := range t.Columns {
		if column.NormalizedName == other.NormalizedName {
            if column.ActualName != other.ActualName {
                fmt.Printf( "Warning: Actual columns do not match %s - %s\n", column.ActualName, other.ActualName )
            }
			return i, column, nil
		}
	}
	return -1, nil, fmt.Errorf("column '%s' not found", other.ActualName)
}

type Column struct {
	ActualName     string
    NormalizedName string
	Type           string
	MaxChars       int64
}

var IDColumn Column = Column {
    ActualName:     "id",
    NormalizedName: "id",
}

func (c *Column) Compatible(other *Column) bool {
	if c.MaxChars == 0 && other.MaxChars == 0 {
		return true
	}

	if c.MaxChars > 0 && other.MaxChars > 0 {
		return c.MaxChars < other.MaxChars
	}

    if (c.Type == "uuid" && (other.Type == "varbinary" || other.Type == "binary") && other.MaxChars == 16) || 
       ((c.Type == "varbinary" || c.Type == "binary") && c.MaxChars == 16 && other.Type == "uuid") {
        return true
    }

	return false
}

func (c *Column) Incompatible(other *Column) bool {
	return !c.Compatible(other)
}

func BuildSchema(db DB) (*Schema, error) {
	rows, err := db.GetSchemaRows()
	if err != nil {
		return nil, err
	}

	data := map[string][]*Column{}
	for rows.Next() {
		var (
			table    sql.NullString
			column   sql.NullString
			datatype sql.NullString
			maxChars sql.NullInt64
		)

		if err := rows.Scan(&table, &column, &datatype, &maxChars); err != nil {
			return nil, err
		}

		data[table.String] = append(data[table.String], &Column{
			ActualName:     column.String,
			NormalizedName: strings.ToLower(column.String),
			Type:           datatype.String,
			MaxChars:       maxChars.Int64,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate through schema rows: %s", err)
	}

	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("failed closing rows: %s", err)
	}

	schema := &Schema{
		Tables: map[string]*Table{},
	}

	for k, v := range data {
        normalizedName := strings.ToLower(k)
		schema.Tables[normalizedName] = &Table{
			ActualName:    k,
            NormalizedName: normalizedName,
			Columns: v,
		}
	}

	return schema, nil
}

func GetIncompatibleColumns(src, dst *Table) ([]*Column, error) {
	var incompatibleColumns []*Column
	for _, dstColumn := range dst.Columns {
		_, srcColumn, err := src.GetColumn(dstColumn)
		if err != nil {
			return nil, fmt.Errorf("failed to find column '%s/%s' in source schema: %s", dst.ActualName, dstColumn.ActualName, err)
		}

		if dstColumn.Incompatible(srcColumn) {
            // fmt.Printf("DEBUG: srcColumn %+v dstColumn %+v\n", srcColumn, dstColumn)
			incompatibleColumns = append(incompatibleColumns, dstColumn)
		}
	}

	return incompatibleColumns, nil
}

func GetIncompatibleRowIDs(db DB, src, dst *Table) ([]int, error) {
	columns, err := GetIncompatibleColumns(src, dst)
	if err != nil {
		return nil, fmt.Errorf("failed getting incompatible columns: %s", err)
	}

	if columns == nil {
		return nil, nil
	}

	limits := make([]string, len(columns))
	for i, column := range columns {
		limits[i] = fmt.Sprintf("LENGTH(%s) > %d", column.ActualName, column.MaxChars)
	}

	stmt := fmt.Sprintf("SELECT id FROM %s WHERE %s", src.ActualName, strings.Join(limits, " OR "))
	rows, err := db.DB().Query(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed getting incompatible row ids: %s", err)
	}

	var rowIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan row: %s", err)
		}
		rowIDs = append(rowIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	return rowIDs, nil
}

func GetIncompatibleRowCount(db DB, src, dst *Table) (int64, error) {
	columns, err := GetIncompatibleColumns(src, dst)
	if err != nil {
		return 0, fmt.Errorf("failed getting incompatible columns: %s", err)
	}

	if columns == nil {
		return 0, nil
	}

	limits := make([]string, len(columns))
	for i, column := range columns {
		limits[i] = fmt.Sprintf("length(%s) > %d", column.ActualName, column.MaxChars)
	}

	stmt := fmt.Sprintf("SELECT count(1) FROM %s WHERE %s", src.ActualName, strings.Join(limits, " OR "))
    // fmt.Printf("DEBUG: GetIncompatibleRowCount: %s\n", stmt)

	var count int64
	err = db.DB().QueryRow(stmt).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func EachMissingRow(src, dst DB, table *Table, dstTable *Table, debug map[string]bool, f func([]interface{})) error {
	srcColumnNamesForSelect := make([]string, len(table.Columns))
	values := make([]interface{}, len(table.Columns))
	scanArgs := make([]interface{}, len(table.Columns))
	colVals := make([]string, len(table.Columns))
    for i := range table.Columns {
        // fmt.Printf( "DEBUG: Columns[%d] = %+v\n", i, table.Columns[i] )
        srcColumnNamesForSelect[i] = src.ColumnNameForSelect(table.Columns[i].ActualName)
		scanArgs[i] = &values[i]
		colVals[i] = dst.ComparisonClause(i, dstTable.Columns[i].ActualName, table.Columns[i].Type)
    }

	// select all rows in src
	stmt := fmt.Sprintf("SELECT %s FROM %s", strings.Join(srcColumnNamesForSelect, ","), table.ActualName)
    //fmt.Printf( "DEBUG SOURCE: \n%s\n", stmt)
    if debug["sql"] {
        fmt.Println("DEBUG SQL:", stmt)
    }

	rows, err := src.DB().Query(stmt)
	if err != nil {
		return fmt.Errorf("failed to select rows: %s", err)
	}

	stmt = fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE %s)`, dstTable.ActualName, strings.Join(colVals, " AND "))
    if debug["sql"] {
        fmt.Println("DEBUG SQL:", stmt)
    }
    //fmt.Printf( "DEBUG DESTINATION: \n%s\n", stmt)
	preparedStmt, err := dst.DB().Prepare(stmt)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %s", err)
	}

	var exists bool
	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan row: %s", err)
		}

		for i := range scanArgs {
			arg := scanArgs[i]
			iface, ok := arg.(*interface{})
			if !ok {
				log.Fatalf("received unexpected type as scanArg: %T (should be *interface{})", arg)
			}

			// replace the precise PostgreSQL time with a less precise MySQL-compatible time
			if t1, ok := (*iface).(time.Time); ok {
				var timeArg interface{} = dst.NormalizeTime(t1)
				scanArgs[i] = &timeArg
			}
		}
        if debug["data"] {
            for i := range scanArgs {
                arg := scanArgs[i]
                iface, ok := arg.(*interface{})
                if !ok {
                    log.Fatalf("received unexpected type as scanArg: %T (should be *interface{})", arg)
                }
                fmt.Printf( "DEBUG scanArgs Source : %d %T  %v  ", i, *iface, *iface )
            }
            fmt.Printf("\n")
        }

		// determine if the row exists in dst
		if err = preparedStmt.QueryRow(scanArgs...).Scan(&exists); err != nil {
			return fmt.Errorf("failed to check if row exists: %s", err)
		}

		if !exists {
			f(scanArgs)
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("failed closing rows: %s", err)
	}

	return nil
}
