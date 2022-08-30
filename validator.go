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

	dstSchema, err := BuildSchema(v.dst)
	if err != nil {
		return nil, fmt.Errorf("failed to build destination schema: %s", err)
	}

    // if ok, _ := StaticSchemaAnalysis( srcSchema, dstSchema ); !ok {
         // return nil, fmt.Errorf("failed static analysis" )
     // }

    if v.debug["schema"] {
        DumpSchema(srcSchema, dstSchema, v.src, v.dst)
    }

    if v.debug["stop"] {
        return nil, fmt.Errorf("user requested a stop operation")
    }

	var results []ValidationResult

    for _, tableName := range  MakeSliceOrderedTableNames(srcSchema.Tables) {
        srcTable := srcSchema.Tables[tableName]
		dstTable, err := dstSchema.GetTable(srcTable.NormalizedName)
		if err != nil {
            return nil, fmt.Errorf("failed to get table from destination schema: %s", err)
        }
        if dstTable.ActualName != srcTable.ActualName {
            fmt.Println( "Warning: Source table", srcTable.ActualName,
                         "does not exist in the destination schema, but found", dstTable.ActualName, "instead.")
		}

		if srcTable.HasIDColumn(dstTable, v.debug) {
			rowIDs, err := GetIncompatibleRowIDs(v.src, srcTable, dstTable, v.debug)
			if err != nil {
				return nil, fmt.Errorf("failed getting incompatible row ids: %s", err)
			}

			results = append(results, ValidationResult{
				TableName:            srcTable.ActualName,
				IncompatibleRowIDs:   rowIDs,
				IncompatibleRowCount: int64(len(rowIDs)),
			})
		} else {
			rowCount, err := GetIncompatibleRowCount(v.src, srcTable, dstTable, v.debug)
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
