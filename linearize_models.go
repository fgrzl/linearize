package linearize

// LinearizedObject is a map of integer keys to any values (used for flattened Protobuf objects)
type LinearizedObject map[int32]any

// LinearizedSlice is a slice of any values (used for Protobuf repeated fields)
type LinearizedSlice map[int32]any

// LinearizedMap is a map of any keys to any values (used for Protobuf map fields)
type LinearizedMap map[int32][2]any
