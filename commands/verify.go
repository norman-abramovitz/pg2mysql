package commands

import (
	"fmt"

	"github.com/tompiscitell/pg2mysql"
)

type VerifyCommand struct{}

func (c *VerifyCommand) Execute([]string) error {
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
	err = pg2mysql.NewVerifier(src, dest, watcher).Verify()
	if err != nil {
		return fmt.Errorf("failed to verify: %s", err)
	}

	return nil
}
