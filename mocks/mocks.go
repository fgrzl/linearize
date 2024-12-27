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

// CreateSuperComplexMessage creates and returns a populated SuperComplex message for testing purposes.
func CreateSuperComplexMessage() *SuperComplex {
	return &SuperComplex{
		Field1: "supercomplex_field1",
		Field2: 500,
		Nested: &Complex{
			Field1: "complex_field1",
			Field2: 200,
			Nested: &Simple{
				Field1:   "simple_nested_field1",
				Field2:   100,
				Repeated: []string{"nested_item1", "nested_item2"},
			},
			Repeated: []Simple{
				{
					Field1:   "complex_repeated_field1",
					Field2:   50,
					Repeated: []string{"item1", "item2"},
				},
			},
			Map: map[int32]*Complex{
				1: {
					Field1: "map_field1",
					Field2: 300,
					Nested: &Simple{
						Field1: "map_nested_field1",
						Field2: 10,
					},
					Repeated: []Simple{
						{
							Field1: "map_repeated_field1",
							Field2: 5,
						},
					},
				},
			},
		},
		Repeated: []Complex{
			{
				Field1: "complex_repeated_field2",
				Field2: 150,
				Nested: &Simple{
					Field1: "nested_repeated_field2",
					Field2: 200,
				},
				Repeated: []Simple{
					{
						Field1: "complex_repeated_item1",
						Field2: 50,
					},
				},
			},
		},
		Map: map[int32]*Complex{
			2: {
				Field1: "complex_map_field2",
				Field2: 400,
				Nested: &Simple{
					Field1: "map_nested_field2",
					Field2: 30,
				},
				Repeated: []Simple{
					{
						Field1: "complex_map_repeated_field2",
						Field2: 20,
					},
				},
			},
		},
	}
}
