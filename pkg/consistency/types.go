package consistency

// ConsistencyLevel defines the consistency level for read/write operations.
type ConsistencyLevel int

const (
	ONE ConsistencyLevel = iota + 1 // Requires one replica
	QUORUM                  // Requires majority of replicas
	ALL                     // Requires all replicas
)

// OperationType specifies the type of operation.
type OperationType int

const (
	READ OperationType = iota
	WRITE
)