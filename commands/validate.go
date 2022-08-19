package commands

import (
	"fmt"

	"pg2mysql"
)

type ValidateCommand struct {
    Debug map[string]bool `short:"d" long:"debug" description:"Set up debug options"` 
}

func (c *ValidateCommand) Execute([]string) error {
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

	results, err := pg2mysql.NewValidator(src, dest, c.Debug).Validate()
	if err != nil {
		return fmt.Errorf("failed to validate: %s", err)
	}

	for _, result := range results {
		switch {
		case len(result.IncompatibleRowIDs) > 0:
			fmt.Printf("found %d incompatible rows in %s with IDs %v\n", result.IncompatibleRowCount, result.TableName, result.IncompatibleRowIDs)

		case result.IncompatibleRowCount > 0:
			fmt.Printf("found %d incompatible rows in %s (which has no 'id' column)\n", result.IncompatibleRowCount, result.TableName)

		default:
			fmt.Printf("%s OK\n", result.TableName)
		}
	}

	return nil
}
