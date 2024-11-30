package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"reflect"
)

const JSONContentType = "application/json"

// ----------------------------------------------------------------------------
// Encoder
// ----------------------------------------------------------------------------

type JSONEncoder struct{}

var _ Encoder = &JSONEncoder{}

func NewJSONEncoder() *JSONEncoder {
	return &JSONEncoder{}
}

func (e *JSONEncoder) Encode(
	ctx context.Context,
	v any,
) (*EncodedValue, error) {
	// Check for nil.
	if v == nil {
		return nil, nil
	}

	// Marshal the value.
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	// Reflect the value.
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}

	// Get the fully qualified name of the type.
	fqn := fullyQualifiedName(rv.Type())

	// Create the content type.
	contentType := mime.FormatMediaType(JSONContentType, map[string]string{"type": fqn})

	// Return the encoded value.
	return &EncodedValue{
		ContentType: contentType,
		Bytes:       b,
	}, nil
}

// ----------------------------------------------------------------------------
// Decoder
// ----------------------------------------------------------------------------

type JSONDecoder struct {
	types map[string]reflect.Type
}

var _ Decoder = &JSONDecoder{}

func NewJSONDecoder(types ...reflect.Type) *JSONDecoder {
	// Create the types map.
	typesMap := make(map[string]reflect.Type)
	for _, typ := range types {
		// If the type is a pointer, get the underlying type.
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}

		// Add the type to the map.
		typesMap[fullyQualifiedName(typ)] = typ
	}

	// Return the decoder.
	return &JSONDecoder{types: typesMap}
}

func (d *JSONDecoder) ContentType() string {
	return JSONContentType
}

func (d *JSONDecoder) Types() map[string]reflect.Type {
	return map[string]reflect.Type{}
}

func (d *JSONDecoder) Decode(
	ctx context.Context,
	encodedValue *EncodedValue,
) (any, error) {
	// Check for nil.
	if encodedValue == nil {
		return nil, nil
	}

	// Parse the content type.
	mediaType, params, err := mime.ParseMediaType(encodedValue.ContentType)
	if err != nil {
		return nil, err
	}

	// Check the media type.
	if mediaType != JSONContentType {
		return nil, fmt.Errorf("unexpected content type: %s", encodedValue.ContentType)
	}

	// Check for a type hint.
	if fqn, ok := params["type"]; ok {
		// Lookup the type by its fully qualified name.
		if typ, ok := d.types[fqn]; ok {
			// Create a new instance of the type.
			v := reflect.New(typ).Interface()

			// Unmarshal the bytes into the new instance.
			if err := json.Unmarshal(encodedValue.Bytes, v); err != nil {
				return v, err
			}

			// Return the decoded value.
			return v, nil
		} else {
			// All type hints must be registered.
			return nil, fmt.Errorf("%w: %s", ErrTypeUnknown, fqn)
		}
	}

	// Unmarshal the payload without a type hint.
	var v any
	if err := json.Unmarshal(encodedValue.Bytes, &v); err != nil {
		return nil, err
	}

	// Return the decoded value.
	return v, nil
}
