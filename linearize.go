package linearize

import (
	"fmt"
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
		key := int32(fd.Number())

		// Handle map fields
		if fd.IsMap() {
			mapValue := make(LinearizedMap, 0)

			// Collect keys to sort them lexicographically
			type keyedValue struct {
				Key   protoreflect.MapKey
				Value protoreflect.Value
			}

			var keys []keyedValue
			value.Map().Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				keys = append(keys, keyedValue{Key: k, Value: v})
				return true
			})

			// Sort keys lexicographically
			sort.SliceStable(keys, func(i, j int) bool {
				return keys[i].Key.String() < keys[j].Key.String()
			})

			// Process the sorted keys and their values
			for _, kv := range keys {
				mapKey := kv.Key.Interface()
				mapVal := kv.Value

				// Check if the map value is a message (i.e., needs linearization)
				if mapVal.Message() != nil {
					// Recursively linearize the nested message
					nestedResult, err := Linearize(mapVal.Message().Interface().(proto.Message))
					if err != nil {
						return false
					}
					mapValue[int32(len(mapValue))] = [2]any{mapKey, nestedResult}
				} else {
					// Handle primitive types
					mapValue[int32(len(mapValue))] = [2]any{mapKey, mapVal.Interface()}
				}
			}

			linearized[key] = mapValue
		} else if fd.IsList() {
			// Handle repeated fields (lists)
			list := make(LinearizedSlice)

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
					list[int32(i)] = nestedResult // Use index as the key in LinearizedSlice
				} else {
					// Append primitive types directly
					list[int32(i)] = elem.Interface() // Use index as the key in LinearizedSlice
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

// Updated Unlinearize function
func Unlinearize(m LinearizedObject, message proto.Message) error {
	v := reflect.ValueOf(message)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("result must be a pointer to a struct")
	}

	msgReflect := message.ProtoReflect().Descriptor()
	elem := v.Elem()
	return unlinearizeStruct(elem, m, msgReflect)
}

// Recursive function to unlinearize structs
func unlinearizeStruct(v reflect.Value, data LinearizedObject, msgReflect protoreflect.MessageDescriptor) error {
	for i, d := range data {
		fd := msgReflect.Fields().ByNumber(protoreflect.FieldNumber(i))
		if fd == nil {
			return fmt.Errorf("field number %d not found in the message", i)
		}

		fieldName := string(fd.Name())
		field := v.FieldByName(fieldName)
		if !field.IsValid() {
			return fmt.Errorf("invalid field name: %s", fieldName)
		}

		if !field.CanSet() {
			return fmt.Errorf("field %s cannot be set", fieldName)
		}

		switch value := d.(type) {
		case LinearizedObject:
			if field.Kind() == reflect.Ptr {
				if field.IsNil() {
					field.Set(reflect.New(field.Type().Elem()))
				}
				field = field.Elem()
			}
			if err := unlinearizeStruct(field, value, fd.Message()); err != nil {
				return fmt.Errorf("failed to unlinearize nested field %s: %w", fieldName, err)
			}

		case LinearizedSlice:
			if field.Kind() != reflect.Slice {
				return fmt.Errorf("expected slice for field %s but got %s", fieldName, field.Kind())
			}
			field.Set(reflect.MakeSlice(field.Type(), len(value), len(value)))
			for j, elem := range value {
				elemValue := field.Index(int(j))
				if err := unlinearizeValue(elemValue, elem, fd); err != nil {
					return fmt.Errorf("failed to set slice element at index %d: %w", j, err)
				}
			}

		case LinearizedMap:
			if field.Kind() != reflect.Map {
				return fmt.Errorf("expected map for field %s but got %s", fieldName, field.Kind())
			}
			field.Set(reflect.MakeMap(field.Type()))
			for _, mapVal := range value {
				kv := mapVal
				key := reflect.ValueOf(kv[0]).Convert(field.Type().Key())
				val := reflect.New(field.Type().Elem()).Elem()
				if err := unlinearizeValue(val, kv[1], fd); err != nil {
					return fmt.Errorf("failed to set map value for key %v: %w", key, err)
				}
				field.SetMapIndex(key, val)
			}

		default:
			if err := unlinearizeValue(field, value, fd); err != nil {
				return fmt.Errorf("failed to set field %s: %w", fieldName, err)
			}
		}
	}
	return nil
}

// Helper to unlinearize a single value
func unlinearizeValue(field reflect.Value, value any, fd protoreflect.FieldDescriptor) error {
	switch fd.Kind() {
	case protoreflect.MessageKind:
		// Ensure the field is settable and is a pointer
		if field.Kind() != reflect.Ptr {
			return fmt.Errorf("expected pointer to a message, but got %s", field.Kind())
		}

		// Create a new instance if the pointer is nil
		if field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
		}

		// Recursively unlinearize the nested message
		nestedMessage := field.Interface().(proto.Message)
		return Unlinearize(value.(LinearizedObject), nestedMessage)

	default:
		// Handle primitive fields
		actualValue := reflect.ValueOf(value)
		if !actualValue.Type().AssignableTo(field.Type()) {
			return fmt.Errorf("type mismatch: expected %s but got %s", field.Type(), actualValue.Type())
		}
		field.Set(actualValue)
	}
	return nil
}
