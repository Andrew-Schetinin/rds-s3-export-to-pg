package source

import "github.com/parquet-go/parquet-go"

// Transformer is an interface for transforming a parquet value into a different type or representation.
type Transformer interface {

	// Transform takes a parquet.Value and converts it into a different type or representation,
	// returning the transformed value or an error.
	Transform(x parquet.Value) (value any, err error)
}
