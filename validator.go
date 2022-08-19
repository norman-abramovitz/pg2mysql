package pg2mysql

import (
	"fmt"
)

type Validator interface {
	Validate() ([]ValidationResult, error)
}

func NewValidator(src, dst DB, debug map[string]bool) Validator {
	return &validator{
		src: src,
		dst: dst,
      debug: debug,
	}
}

type validator struct {
	src, dst DB
    debug map[string]bool
}

func (v *validator) Validate() ([]ValidationResult, error) {
	srcSchema, err := BuildSchema(v.src)
	if err != nil {
		return nil, fmt.Errorf("failed to build source schema: %s", err)
	}

    if v.debug["schema"] {
        fmt.Printf("DEBUG SOURCE SCHEMA: %v\n", srcSchema)
    }

	dstSchema, err := BuildSchema(v.dst)
	if err != nil {
		return nil, fmt.Errorf("failed to build destination schema: %s", err)
	}

    if v.debug["schema"] {
        fmt.Printf("DEBUG DESTINATION SCHEMA: %v\n", dstSchema)
    }


	var results []ValidationResult
	for _, srcTable := range srcSchema.Tables {
		dstTable, err := dstSchema.GetTable(srcTable.NormalizedName)
		if err != nil {
            return nil, fmt.Errorf("failed to get table from destination schema: %s", err)
        }
        if dstTable.ActualName != srcTable.ActualName {
            fmt.Println( "Warning: Source table", srcTable.ActualName,
                         "does not exist in the destination schema, but found", dstTable.ActualName, "instead.")
		}

		if srcTable.HasColumn(&IDColumn) {
			rowIDs, err := GetIncompatibleRowIDs(v.src, srcTable, dstTable)
			if err != nil {
				return nil, fmt.Errorf("failed getting incompatible row ids: %s", err)
			}

			results = append(results, ValidationResult{
				TableName:            srcTable.ActualName,
				IncompatibleRowIDs:   rowIDs,
				IncompatibleRowCount: int64(len(rowIDs)),
			})
		} else {
			rowCount, err := GetIncompatibleRowCount(v.src, srcTable, dstTable)
			if err != nil {
				return nil, fmt.Errorf("failed getting incompatible row count: %s", err)
			}

			results = append(results, ValidationResult{
				TableName:            srcTable.ActualName,
				IncompatibleRowCount: rowCount,
			})
		}
	}

	return results, nil
}

type ValidationResult struct {
	TableName            string
	IncompatibleRowIDs   []int
	IncompatibleRowCount int64
}
