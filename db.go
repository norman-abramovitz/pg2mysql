package pg2mysql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
    "sort"
	"time"
)

type DB interface {
	Open() error
	Close() error
    GetDbName() string
    GetDriverName() string
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

func DumpColumns(src, dst []*Column) {
    var srcPad, dstPad int = 45, 45
    var srcIdx, dstIdx int

    srcSide := fmt.Sprintf( "    Columns: %d", len(src))
    dstSide := fmt.Sprintf( "    Columns: %d", len(dst))
    fmt.Printf("%-*s%-*s\n", srcPad, srcSide, dstPad, dstSide)

    for srcIdx < len(src)  || dstIdx < len(dst) {
        if srcIdx >= len(src) {
            dstColumn := dst[dstIdx]
            dstSide = fmt.Sprintf( "    %3d: %s", dstIdx, dstColumn.ActualName)
            fmt.Printf("%-*s%-*s\n", srcPad, "", dstPad, dstSide)
            dstColumnLength := ""
            if dstColumn.MaxChars != 0 {
                dstColumnLength = fmt.Sprintf("(%d)", dstColumn.MaxChars);
            }
            dstSide = fmt.Sprintf( "         %s%s", dstColumn.Type, dstColumnLength)
            fmt.Printf("%-*s%-*s\n", srcPad, "", dstPad, dstSide)
            dstIdx++
            continue
        }
        if dstIdx >= len(dst) {
            srcColumn := src[srcIdx]
            srcSide = fmt.Sprintf( "    %3d: %s", srcIdx, srcColumn.ActualName)
            fmt.Printf("%-*s\n", srcPad, srcSide)
            srcColumnLength := ""
            if srcColumn.MaxChars != 0 {
                srcColumnLength = fmt.Sprintf("(%d)", srcColumn.MaxChars);
            }
            srcSide = fmt.Sprintf( "         %s%s", srcColumn.Type, srcColumnLength)
            fmt.Printf("%-*s\n", srcPad, srcSide)
            srcIdx++
            continue
        }
        
        switch  strings.Compare(src[srcIdx].NormalizedName, dst[dstIdx].NormalizedName) {
        case 0:
            srcColumn := src[srcIdx]
            dstColumn := dst[dstIdx]
            srcSide = fmt.Sprintf( "    %3d: %s", srcIdx, srcColumn.ActualName)
            dstSide = fmt.Sprintf( "    %3d: %s", dstIdx, dstColumn.ActualName)
            fmt.Printf("%-*s%-*s\n", srcPad, srcSide, dstPad, dstSide)

            srcColumnLength := ""
            if srcColumn.MaxChars != 0 {
                srcColumnLength = fmt.Sprintf("(%d)", srcColumn.MaxChars);
            }
            dstColumnLength := ""
            if dstColumn.MaxChars != 0 {
                dstColumnLength = fmt.Sprintf("(%d)", dstColumn.MaxChars);
            }

            srcSide = fmt.Sprintf( "         %s%s", srcColumn.Type, srcColumnLength)
            dstSide = fmt.Sprintf( "         %s%s", dstColumn.Type, dstColumnLength)
            fmt.Printf("%-*s%-*s\n", srcPad, srcSide, dstPad, dstSide)

            StaticColumnAnalysis(srcColumn, dstColumn)
            srcIdx++
            dstIdx++
        case 1:
            dstColumn := dst[dstIdx]
            dstSide = fmt.Sprintf( "    %3d: %s", dstIdx, dstColumn.ActualName)
            fmt.Printf("%-*s%-*s\n", srcPad, "", dstPad, dstSide)
            dstColumnLength := ""
            if dstColumn.MaxChars != 0 {
                dstColumnLength = fmt.Sprintf("(%d)", dstColumn.MaxChars);
            }
            dstSide = fmt.Sprintf( "         %s%s", dstColumn.Type, dstColumnLength)
            fmt.Printf("%-*s%-*s\n", srcPad, "", dstPad, dstSide)
            dstIdx++
        case -1:
            srcColumn := src[srcIdx]
            srcSide = fmt.Sprintf( "    %3d: %s", srcIdx, srcColumn.ActualName)
            fmt.Printf("%-*s\n", srcPad, srcSide)
            srcColumnLength := ""
            if srcColumn.MaxChars != 0 {
                srcColumnLength = fmt.Sprintf("(%d)", srcColumn.MaxChars);
            }
            srcSide = fmt.Sprintf( "         %s%s", srcColumn.Type, srcColumnLength)
            fmt.Printf("%-*s\n", srcPad, srcSide)
            srcIdx++
        }
    }
}

func DumpSchema( src, dst *Schema, srcDB, dstDB DB) {
    var srcPad, dstPad int = 45, 45

    srcSide := fmt.Sprintf( "Driver: %s", srcDB.GetDriverName())
    dstSide := fmt.Sprintf( "Driver: %s", dstDB.GetDriverName())
    fmt.Printf("%-*s%-*s\n", srcPad, srcSide, dstPad, dstSide)

    srcSide = fmt.Sprintf( "Database: %s", srcDB.GetDbName())
    dstSide = fmt.Sprintf( "Database: %s", dstDB.GetDbName())
    fmt.Printf("%-*s%-*s\n", srcPad, srcSide, dstPad, dstSide)

    var srcIdx, dstIdx int
    srcTableNames := MakeSliceOrderedTableNames(src.Tables)
    dstTableNames := MakeSliceOrderedTableNames(dst.Tables)

    for srcIdx < len(srcTableNames)  || dstIdx < len(dstTableNames) {
        if srcIdx >= len(srcTableNames) {
            dstTable := dst.Tables[dstTableNames[dstIdx]]
            dstSide = fmt.Sprintf( "  Table: %s", dstTable.ActualName)
            fmt.Printf("%-*s%-*s\n", srcPad, "", dstPad, dstSide)
            DumpColumns(make([]*Column, 0), dstTable.Columns)
            dstIdx++
            continue
        }
        if dstIdx >= len(dstTableNames) {
            srcTable := src.Tables[srcTableNames[srcIdx]]
            srcSide = fmt.Sprintf( "  Table: %s", srcTable.ActualName)
            fmt.Printf("%-*s\n", srcPad, srcSide)
            DumpColumns(srcTable.Columns, make([]*Column, 0))
            srcIdx++
            continue
        }

        
        switch  strings.Compare(srcTableNames[srcIdx], dstTableNames[dstIdx]) {
        case 0:
            srcTable := src.Tables[srcTableNames[srcIdx]]
            dstTable := dst.Tables[dstTableNames[dstIdx]]
            srcSide = fmt.Sprintf( "  Table: %s", srcTable.ActualName)
            dstSide = fmt.Sprintf( "  Table: %s", dstTable.ActualName)
            fmt.Printf("%-*s%-*s\n", srcPad, srcSide, dstPad, dstSide)
            DumpColumns(srcTable.Columns, dstTable.Columns)
            srcIdx++
            dstIdx++
        case 1:
            dstTable := dst.Tables[dstTableNames[dstIdx]]
            dstSide = fmt.Sprintf( "  Table: %s", dstTable.ActualName)
            fmt.Printf("%-*s%-*s\n", srcPad, "", dstPad, dstSide)
            DumpColumns(make([]*Column, 0), dstTable.Columns)
            dstIdx++
        case -1:
            srcTable := src.Tables[srcTableNames[srcIdx]]
            srcSide = fmt.Sprintf( "  Table: %s", srcTable.ActualName)
            fmt.Printf("%-*s\n", srcPad, srcSide)
            DumpColumns(srcTable.Columns, make([]*Column, 0))
            srcIdx++
        }
    }
}

func (s *Schema) DumpSchema() {
    for key, table := range s.Tables {
        fmt.Printf("Schema\n")
        fmt.Printf("    Table: %s\n", key)
        fmt.Printf("        ActualName:     %s\n", table.ActualName)
        fmt.Printf("        NormalizedName: %s\n", table.NormalizedName)
        fmt.Printf("        Columns:\n")
        DumpTableColumns( 12, table.Columns ) 
    }
}

func DumpTableColumns( indent int, columns []*Column ) {
    for i, column := range columns { 
        fmt.Printf("%*s%4d: Name: %s %s\n", indent, "", i, column.ActualName, column.NormalizedName)
        fmt.Printf("%*sType: %v %v\n", indent + 8, "", column.Type, column.MaxChars)
    }
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

func (t *Table) HasIDColumn(other *Table) bool {
    ti, tcolumn, _ := t.GetColumn(&IDColumn)
    oi, ocolumn, _ := other.GetColumn(&IDColumn)
    if ti >= 0 && oi >= 0 {
        fmt.Printf( "DEBUG %s HasIDColumn %+v %+v\n", t.NormalizedName, tcolumn, ocolumn)
    }
    return false;
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

func MakeSliceOrderedTableNames( tables map[string]*Table ) ([]string) {

    var i int
    tableList := make([]string, len(tables)) 

    for name := range tables {
        tableList[i] = name
        i++
    }
    sort.Strings(tableList)
    return tableList
}

func differenceSlices( v1 []string, v2 []string ) {

    var v1i, v2i int 

    fmt.Println(v1i, len(v1), v2i, len(v2)) 

    for v1i < len(v1)  || v2i < len(v2) {
        fmt.Println(v1i, len(v1), v2i, len(v2)) 
        if v1i >= len(v1) {
            fmt.Println("slice two has ", v2[v2i], "and it does not exist in slice one" )
            v2i++
            continue
        }
        if v2i >= len(v2) {
            fmt.Println("slice one  has ", v1[v1i], "and it does not exist in slice two")
            v1i++
            continue
        }
        
        switch  strings.Compare(v1[v1i], v2[v2i]) {
        case 0:
            fmt.Println("Both slices have ", v2[v2i] )
            v1i++
            v2i++
        case 1:
            fmt.Print("slice two has ", v2[v2i], "and it does not exist in slice one" )
            v2i++
        case -1:
            fmt.Print("slice one  has ", v1[v1i], "and it does not exist in slice two")
            v1i++
        }
    }
}

func StaticColumnAnalysis( src, dst *Column) int {
    switch {
        case src.Type == dst.Type && src.MaxChars == dst.MaxChars,
             src.Type == "integer" && dst.Type == "int",   
             src.Type == "character varying" && (dst.Type == "varchar" || dst.Type == "text") && src.MaxChars <= dst.MaxChars,
             src.Type == "text" && (dst.Type == "text" || dst.Type == "mediumtext" || dst.Type == "longtext") && src.MaxChars == 0 && dst.MaxChars >= 65535,
             src.Type == "character" && dst.Type == "char" && src.MaxChars == dst.MaxChars && src.MaxChars > 0,
             src.Type == "boolean" && dst.Type == "tinyint" && dst.MaxChars == 0,
             src.Type == "bytea" && dst.Type == "mediumblob" && src.MaxChars == 0 && 1024 * 1024 <= dst.MaxChars:
                return 0
            case src.Type == "uuid" && (dst.Type == "binary" || dst.Type == "varbinary") && dst.MaxChars == 16,
                 src.Type == "timestamp with time zone" && dst.Type == "datetime",
                 src.Type == "timestamp without time zone" && dst.Type == "datetime",
                 src.Type == "timestamp without time zone" && dst.Type == "timestamp":
                return 1
        default:
            fmt.Printf("EVALUATE: %s(%d)  %s(%d)\n", src.Type, src.MaxChars, dst.Type, dst.MaxChars) 
            return 2
    }
}

func StaticSchemaAnalysis( src, dst *Schema) (bool, error) {

    srcKeys := MakeSliceOrderedTableNames( src.Tables )
    srcKeys = append(srcKeys, "z1" )
    dstKeys := MakeSliceOrderedTableNames( dst.Tables )
    dstKeys = append(dstKeys, "z0" )

    differenceSlices( srcKeys, dstKeys )

    return true, nil
}

func GetIncompatibleColumns(src, dst *Table) ([]*Column, error) {
	var incompatibleColumns []*Column
	for _, dstColumn := range dst.Columns {
		_, srcColumn, err := src.GetColumn(dstColumn)
		if err != nil {
			return nil, fmt.Errorf("failed to find column '%s/%s' in source schema: %s", dst.ActualName, dstColumn.ActualName, err)
		}

		if dstColumn.Incompatible(srcColumn) {
            fmt.Printf("DEBUG: srcColumn %+v dstColumn %+v\n", srcColumn, dstColumn)
			incompatibleColumns = append(incompatibleColumns, srcColumn)
		}
	}

	return incompatibleColumns, nil
}

func GetIncompatibleRowIDs(db DB, src, dst *Table, debug map[string]bool) ([]int, error) {
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
    if debug["sql"] {
        fmt.Println("DEBUG GetIncompatibleRowIDs SQL:", stmt)
    }

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

func GetIncompatibleRowCount(db DB, src, dst *Table, debug map[string]bool) (int64, error) {
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
    if debug["sql"] {
        fmt.Println("DEBUG GetIncompatibleRowCount SQL:", stmt)
    }

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
                if debug["datetime"] {
                    fmt.Println( "DEBUG BEFORE NormalizeTime", t1)
                }
				var timeArg interface{} = dst.NormalizeTime(t1)
				scanArgs[i] = &timeArg
                if debug["datetime"] {
                    fmt.Println( "DEBUG AFTER  NormalizeTime", t1, "scanArg ", i , timeArg)
                }
			}
		}
        if debug["data"] {
            for i := range scanArgs {
                arg := scanArgs[i]
                iface, ok := arg.(*interface{})
                if !ok {
                    log.Fatalf("received unexpected type as scanArg: %T (should be *interface{})", arg)
                }
                fmt.Printf( "DEBUG scanArgs Source : %d %T  %v\n", i, *iface, *iface )
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
