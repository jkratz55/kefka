package kefka

type AutoOffsetReset string

const (
	Earliest AutoOffsetReset = "earliest"
	Latest   AutoOffsetReset = "latest"
)
