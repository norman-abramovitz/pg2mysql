package pg2mysql

import (
	"fmt"

	"github.com/google/uuid"
)

type Verifier interface {
	Verify() error
}

type verifier struct {
	src, dst DB
    debug map[string]bool
	watcher  VerifierWatcher
}

func NewVerifier(src, dst DB, debug map[string]bool, watcher VerifierWatcher) Verifier {
	return &verifier{
		src:     src,
		dst:     dst,
        debug:   debug,
		watcher: watcher,
	}
}

func (v *verifier) Verify() error {
	srcSchema, err := BuildSchema(v.src)
	if err != nil {
		return fmt.Errorf("failed to build source schema: %s", err)
	}

	dstSchema, err := BuildSchema(v.dst)
	if err != nil {
		return fmt.Errorf("failed to build source schema: %s", err)
	}

    for _, tableName := range  MakeSliceOrderedTableNames(srcSchema.Tables) {
        srcTable := srcSchema.Tables[tableName]
		v.watcher.TableVerificationDidStart(srcTable.ActualName)

		dstTable, err := dstSchema.GetTable(srcTable.NormalizedName)
		if err != nil {
            return fmt.Errorf("failed to get table from destination schema: %s", err)
        }

		var missingRows int64
		var missingIDs []string
		err = EachMissingRow(v.src, v.dst, srcTable, dstTable, v.debug, func(scanArgs []interface{}) {
			if colIndex, _, getColErr := srcTable.GetColumn(&IDColumn); getColErr == nil {
				if colID, ok := scanArgs[colIndex].(*interface{}); ok {
					missingIDs = append(missingIDs, ColIDToString(*colID))
				}
			}
			missingRows++
		})
		if err != nil {
			v.watcher.TableVerificationDidFinishWithError(srcTable.ActualName, err)
			continue
		}

		v.watcher.TableVerificationDidFinish(srcTable.ActualName, missingRows, missingIDs)
	}

	return nil
}

func ColIDToString(colID interface{}) string {
	switch v := colID.(type) {
	case []byte:
		var u uuid.UUID
		s := fmt.Sprintf("%v", v)

		if len(v) == 16 {
			u, err := uuid.FromBytes(v)
			if err == nil {
				s = u.String()
			}
			return u.String()
		} else if len(v) == 36 {
			err := u.UnmarshalText(v)
			if err == nil {
				s = u.String()
			}
		}
		return s
	default:
		return fmt.Sprintf("%v", v)
	}
}
