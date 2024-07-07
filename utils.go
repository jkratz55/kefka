package kefka

// StringPtr returns a pointer to the string passed in. This is useful since
// the official confluent-kafka-go library uses pointers to strings in many
// places like topics and Go doesn't allow you to take the address of a string
// in a single statement.
func StringPtr(s string) *string {
	return &s
}
