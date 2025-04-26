package query

import (
	"ddb/pkg/consistency"
)


type QueryType int

const (
	PUT QueryType = iota
	GET
	DELETE
)


type Query struct {
	Type            QueryType
	Key             string
	Value           string // Only used for PUT
	ConsistencyLevel consistency.ConsistencyLevel
}