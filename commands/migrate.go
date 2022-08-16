package commands

import (
	"fmt"

	"pg2mysql"
)

type MigrateCommand struct {
	Truncate bool `long:"truncate" description:"Truncate destination tables before migrating data"`
}

func (c *MigrateCommand) Execute([]string) error {
	var dest pg2mysql.DB

	if PG2MySQL.Config.Dest.Flavor == "mysql" {
		dest = pg2mysql.NewMySQLDB(
			PG2MySQL.Config.Dest.Database,
			PG2MySQL.Config.Dest.Username,
			PG2MySQL.Config.Dest.Password,
			PG2MySQL.Config.Dest.Host,
			PG2MySQL.Config.Dest.Port,
			PG2MySQL.Config.Dest.RoundTime,
		)
	} else if PG2MySQL.Config.Dest.Flavor == "psql" {
		dest = pg2mysql.NewPostgreSQLDB(
			PG2MySQL.Config.Dest.Database,
			PG2MySQL.Config.Dest.Username,
			PG2MySQL.Config.Dest.Password,
			PG2MySQL.Config.Dest.Host,
			PG2MySQL.Config.Dest.Port,
			PG2MySQL.Config.Dest.SSLMode,
		)
	}

	err := dest.Open()
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %s", err)
	}
	defer dest.Close()

	src := pg2mysql.NewPostgreSQLDB(
		PG2MySQL.Config.Source.Database,
		PG2MySQL.Config.Source.Username,
		PG2MySQL.Config.Source.Password,
		PG2MySQL.Config.Source.Host,
		PG2MySQL.Config.Source.Port,
		PG2MySQL.Config.Source.SSLMode,
	)
	err = src.Open()
	if err != nil {
		return fmt.Errorf("failed to open pg connection: %s", err)
	}
	defer src.Close()

	watcher := pg2mysql.NewStdoutPrinter()
	err = pg2mysql.NewMigrator(src, dest, c.Truncate, watcher).Migrate()
	if err != nil {
		return fmt.Errorf("failed migrating: %s", err)
	}

	return nil
}
