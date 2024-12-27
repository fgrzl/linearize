package linearize

import (
	"fmt"
	"hash/crc32"
	"reflect"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Linearize recursively flattens a Protobuf message into a LinearizedObject.
func Linearize(message proto.Message) (LinearizedObject, error) {
	linearized := make(LinearizedObject)

	// Return an empty LinearizedObject for nil message
	if message == nil {
		return linearized, nil
	}

	// Use reflection to inspect the message fields
	msgReflect := message.ProtoReflect()

	// Iterate over the fields of the message
	msgReflect.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		key := int32(fd.Number()) // Use field number as the key

		// Handle map fields
		if fd.IsMap() {
			mapValue := make(LinearizedMap, 0)

			// Collect and sort keys by CRC32 hash for consistent order
			type keyedValue struct {
				Key   protoreflect.MapKey
				CRC32 uint32
			}

			var keys []keyedValue
			value.Map().Range(func(k protoreflect.MapKey, _ protoreflect.Value) bool {
				hash := crc32.ChecksumIEEE([]byte(k.String()))
				keys = append(keys, keyedValue{Key: k, CRC32: hash})
				return true
			})

			sort.Slice(keys, func(i, j int) bool {
				return keys[i].CRC32 < keys[j].CRC32
			})

			for _, kv := range keys {
				mapKey := kv.Key
				mapVal := value.Map().Get(mapKey)

				// Directly use the map key without linearization (it's a valid scalar or enum)
				keyInterface := mapKey.Interface()

				// Check if the map value is a message (i.e., needs linearization)
				if mapVal.Message() != nil {
					// Recursively linearize the nested message
					nestedResult, err := Linearize(mapVal.Message().Interface().(proto.Message))
					if err != nil {
						return false
					}
					mapValue = append(mapValue, [2]any{keyInterface, nestedResult})
				} else {
					// Handle primitive types
					mapValue = append(mapValue, [2]any{keyInterface, mapVal.Interface()})
				}
			}

			linearized[key] = mapValue
		} else if fd.IsList() {
			// Handle repeated fields (lists)
			var list LinearizedArray
			for i := 0; i < value.List().Len(); i++ {
				elem := value.List().Get(i)

				if fd.Kind() == protoreflect.MessageKind {
					// Recursively linearize nested message elements
					nestedMessage, ok := elem.Message().Interface().(proto.Message)
					if !ok {
						return false // Ensure type assertion succeeded
					}

					nestedResult, err := Linearize(nestedMessage)
					if err != nil {
						return false
					}
					list = append(list, nestedResult)
				} else {
					// Append primitive types directly
					list = append(list, elem.Interface())
				}
			}
			linearized[key] = list
		} else if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			// Recursively handle nested messages
			nestedMessage := value.Message().Interface()
			nestedResult, err := Linearize(nestedMessage.(proto.Message))
			if err != nil {
				return false
			}
			linearized[key] = nestedResult
		} else {
			// Handle primitive fields
			linearized[key] = value.Interface()
		}
		return true
	})

	return linearized, nil
}

// Unlinearize function for decoding LinearizedObject into the correct struct type
func Unlinearize(m LinearizedObject, message proto.Message) error {
	v := reflect.ValueOf(message)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("result must be a pointer to a struct")
	}
	return unlinearizeStruct(v.Elem(), m)
}

func unlinearizeStruct(v reflect.Value, data LinearizedObject) error {
	// Iterate through the LinearizedObject by position and set the corresponding fields in the struct
	for i, d := range data {
		pos := int(i)
		// Get the struct field at the given position
		if pos >= v.NumField() {
			return fmt.Errorf("invalid position %d: exceeds struct field count", i)
		}

		// Get the field by position (using v.Field(pos))
		field := v.Field(pos)
		if !field.IsValid() {
			return fmt.Errorf("invalid field for position: %d", i)
		}

		// Ensure the field is addressable (can be set)
		if !field.CanSet() {
			return fmt.Errorf("field at position %d cannot be set", i)
		}

		// Handle the field depending on its type
		switch value := d.(type) {
		case LinearizedObject:
			// Handle nested struct (LinearizedObject is a map of field numbers)
			if field.Kind() == reflect.Ptr {
				if field.IsNil() {
					field.Set(reflect.New(field.Type().Elem()))
				}
				field = field.Elem()
			}
			if err := unlinearizeStruct(field, value); err != nil {
				return err
			}

		case LinearizedArray:
			// Handle repeated fields (slices)
			if field.Kind() != reflect.Slice {
				return fmt.Errorf("expected slice field at position %d, but got %s", i, field.Kind())
			}
			if field.IsNil() {
				field.Set(reflect.MakeSlice(field.Type(), len(value), len(value)))
			}

			// Iterate over the LinearizedArray elements
			for j, elem := range value {
				elemValue := field.Index(j)

				if elemStruct, ok := elem.(LinearizedObject); ok {
					// Handle struct or pointer to struct
					if elemValue.Kind() == reflect.Ptr {
						// Initialize pointer if nil
						if elemValue.IsNil() {
							elemValue.Set(reflect.New(elemValue.Type().Elem()))
						}
						// Recursively populate the struct
						if err := unlinearizeStruct(elemValue.Elem(), elemStruct); err != nil {
							return fmt.Errorf("failed to unlinearize struct in slice at index %d: %w", j, err)
						}
					} else if elemValue.Kind() == reflect.Struct {
						// Directly populate the struct
						if err := unlinearizeStruct(elemValue, elemStruct); err != nil {
							return fmt.Errorf("failed to unlinearize struct in slice at index %d: %w", j, err)
						}
					} else {
						return fmt.Errorf("unsupported slice element type at index %d: %s", j, elemValue.Kind())
					}
				} else {
					// Handle primitive or non-struct types
					actualValue := reflect.ValueOf(elem)

					// Attempt conversion if necessary
					if !actualValue.Type().AssignableTo(elemValue.Type()) {
						convertedValue := actualValue.Convert(elemValue.Type())
						if convertedValue.IsValid() {
							elemValue.Set(convertedValue)
						} else {
							return fmt.Errorf("type mismatch in slice at index %d: expected %s but got %s", j, elemValue.Type(), actualValue.Type())
						}
					} else {
						// Set the value directly if assignable
						elemValue.Set(actualValue)
					}
				}
			}

		case LinearizedMap:
			// Handle map fields
			if field.Kind() != reflect.Map {
				return fmt.Errorf("expected map field at position %d, but got %s", i, field.Kind())
			}
			// Initialize the map if it's a pointer and nil
			if field.Kind() == reflect.Ptr && field.IsNil() {
				field.Set(reflect.New(field.Type().Elem()))
			}
			mapValue := field
			for mapKey, mapVal := range value {
				mapValue.SetMapIndex(reflect.ValueOf(mapKey), reflect.ValueOf(mapVal))
			}

		default:
			// Handle primitive fields
			if field.Kind() != reflect.TypeOf(value).Kind() {
				return fmt.Errorf("expected %s field at position %d, but got %s", reflect.TypeOf(value).Kind(), i, field.Kind())
			}
			field.Set(reflect.ValueOf(value))
		}
	}

	return nil
}
