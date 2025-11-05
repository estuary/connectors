package main

import (
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
)

const (
	// Max string size in bytes for the varchar data type.
	MaxCharSizeBytes = 8000

	// Max string size in byte-pairs for the nvarchar data type.
	MaxNCharSizeBytePairs = 4000

	// MaxPKStringSize is the largest allowed size of a Primary Key.
	MaxPKStringSize = 900

	// MaxStringSize is a special value which indictes the column should use
	// the MAX storage.
	MaxStringSize = -1
)

// SizedText is compatible with any column with the same
// ColumnType and has at least SizeByte reserved for the string.
type SizedText struct {
	// Type of the column, varchar or nvarchar
	ColumnType string

	// Size reserved for the string.  For varchar's this is the number of
	// bytes, for nvarchars this is the number of byte-pairs.  The special
	// value -1 indicates that the column should use the MAX width.
	Size int

	// Collation of the column.
	Collation string
}

func (s *SizedText) DDL() string {
	var size string
	if s.Size == MaxStringSize {
		size = "MAX"
	} else {
		size = strconv.Itoa(s.Size)
	}
	return fmt.Sprintf("%s(%s) COLLATE %s", s.ColumnType, size, s.Collation)
}

func (s *SizedText) Compatible(existing boilerplate.ExistingField) bool {
	if !strings.EqualFold(existing.Type, s.ColumnType) {
		return false
	}

	// The column was created with MAX as the string size, which reserves
	// 2^31-1 bytes for storage.
	if existing.CharacterMaxLength == MaxStringSize {
		return true
	}

	return existing.CharacterMaxLength >= s.Size
}

var _ sql.CompatibleColumnType = (*SizedText)(nil)

// MapSizedText creates a mapping function for strings that may have limited string size.
func MapSizedText(stringType string, limit int, collation string) sql.MapProjectionFn {
	var maxStringSize uint32
	var bytesPerCharacter uint32
	if stringType == "varchar" {
		maxStringSize = MaxCharSizeBytes
		bytesPerCharacter = 4
	} else { // nvarchar
		maxStringSize = MaxNCharSizeBytePairs
		bytesPerCharacter = 1
	}

	return func(p *sql.Projection) (string, sql.CompatibleColumnTypes, sql.ElementConverter) {
		high, requiredSizeBytes := bits.Mul32(p.Inference.String_.MaxLength, bytesPerCharacter)
		if high != 0 {
			requiredSizeBytes = math.MaxUint32
		}

		if p.Inference.String_ == nil || requiredSizeBytes == 0 || requiredSizeBytes > maxStringSize {
			column := &SizedText{
				ColumnType: stringType,
				Size:       limit,
				Collation:  collation,
			}
			return column.DDL(), []sql.CompatibleColumnType{column}, nil
		}

		column := &SizedText{
			ColumnType: stringType,
			Size:       int(requiredSizeBytes),
			Collation:  collation,
		}
		return column.DDL(), []sql.CompatibleColumnType{column}, nil
	}
}
