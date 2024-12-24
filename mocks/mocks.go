package mocks

// CreateSimpleMessage returns a simple mock message
func CreateSimpleMessage() *Simple {
	return &Simple{
		Field1:   "test1",
		Field2:   42,
		Repeated: []string{"value1", "value2"},
	}
}

// CreateComplexMessage returns a complex mock message
func CreateComplexMessage() *Complex {
	return &Complex{
		Field1: "complex_field1",
		Field2: 100,
		Nested: CreateSimpleMessage(),
		Repeated: []*Simple{
			CreateSimpleMessage(),
			CreateSimpleMessage(),
		},
		Map: map[string]*Simple{
			"key1": CreateSimpleMessage(),
			"key2": CreateSimpleMessage(),
		},
	}
}
