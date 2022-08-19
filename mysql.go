package pg2mysql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

func NewMySQLDB(
	database string,
	username string,
	password string,
	host string,
	port int,
	roundTime bool,
) DB {
	config := mysql.NewConfig()
	config.User = username
	config.Passwd = password
	config.DBName = database
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("%s:%d", host, port)
	config.MultiStatements = true
	config.Params = map[string]string{
		"charset":   "utf8",
		"parseTime": "True",
	}

	return &mySQLDB{
		dsn:       config.FormatDSN(),
		dbName:    database,
		roundTime: roundTime,
	}
}

type mySQLDB struct {
	dsn       string
	db        *sql.DB
	dbName    string
	roundTime bool
}

func (m *mySQLDB) Open() error {
	db, err := sql.Open("mysql", m.dsn)
	if err != nil {
		return err
	}

	m.db = db

	return nil
}

func (m *mySQLDB) Close() error {
	return m.db.Close()
}

func (m *mySQLDB) GetSchemaRows() (*sql.Rows, error) {
	query := `
	SELECT table_name,
				 column_name,
				 data_type,
				 character_maximum_length
	FROM   information_schema.columns
	WHERE  table_schema = ?
    ORDER BY table_name, column_name
    COLLATE utf8_bin`
	rows, err := m.db.Query(query, m.dbName)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (m *mySQLDB) DB() *sql.DB {
	return m.db
}

func (m *mySQLDB) ColumnNameForSelect(name string) string {
	return fmt.Sprintf("`%s`", name)
}

func (m *mySQLDB) EnableConstraints() error {
	_, err := m.db.Exec("SET FOREIGN_KEY_CHECKS = 1;")
	return err
}

func (m *mySQLDB) DisableConstraints() error {
	_, err := m.db.Exec("SET FOREIGN_KEY_CHECKS = 0;")
	return err
}

func (m *mySQLDB) NormalizeTime(t time.Time) time.Time {
	if m.roundTime {
		return t.Round(time.Second)
	}

	return t.Truncate(time.Second)
}

func (m *mySQLDB) ParameterMarker(paramIndex int) string {
	return "?"
}

func (m *mySQLDB) ComparisonClause(paramIndex int, columnName string, columnType string) string {
    clause := fmt.Sprintf("%s <=> %s", m.ColumnNameForSelect(columnName), m.ParameterMarker(paramIndex))
    if columnType == "uuid" {
        clause = fmt.Sprintf("%s <=> %s", m.ColumnNameForSelect(columnName), "unhex(replace(" +m.ParameterMarker(paramIndex) + ",'-',''))")
    }
    return clause
}
