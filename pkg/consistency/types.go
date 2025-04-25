package consistency 

type ConsistencyLevel int

const (
	ONE ConsistencyLevel = iota + 1
	QUORUM
	ALL
)

type OperationType int

const (
	READ OperationType = iota
	WRITE
)
