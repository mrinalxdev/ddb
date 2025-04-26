package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"ddb/pkg/consistency"
	"ddb/pkg/partition"
)

type QueryProcessor struct {
	coordinator *consistency.Coordinator
	partitioner *partition.Partitioner
	nodeClient  consistency.NodeClient
	timeout     time.Duration
}


func NewQueryProcessor(coordinator *consistency.Coordinator, partitioner *partition.Partitioner, nodeClient consistency.NodeClient, timeout time.Duration) *QueryProcessor {
	return &QueryProcessor{
		coordinator: coordinator,
		partitioner: partitioner,
		nodeClient:  nodeClient,
		timeout:     timeout,
	}
}


func (qp *QueryProcessor) ParseQuery(input string) (*Query, error) {
	parts := strings.Fields(input)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid query: too few arguments")
	}

	query := &Query{
		ConsistencyLevel: consistency.QUORUM, // Default consistency
	}

	switch strings.ToUpper(parts[0]) {
	case "PUT":
		if len(parts) != 3 {
			return nil, fmt.Errorf("PUT requires key and value")
		}
		query.Type = PUT
		query.Key = parts[1]
		query.Value = parts[2]
	case "GET":
		if len(parts) != 2 {
			return nil, fmt.Errorf("GET requires key")
		}
		query.Type = GET
		query.Key = parts[1]
	case "DELETE":
		if len(parts) != 2 {
			return nil, fmt.Errorf("DELETE requires key")
		}
		query.Type = DELETE
		query.Key = parts[1]
	default:
		return nil, fmt.Errorf("unknown query type: %s", parts[0])
	}

	
	if len(parts) > queryArgCount(query.Type) {
		switch strings.ToUpper(parts[queryArgCount(query.Type)]) {
		case "ONE":
			query.ConsistencyLevel = consistency.ONE
		case "QUORUM":
			query.ConsistencyLevel = consistency.QUORUM
		case "ALL":
			query.ConsistencyLevel = consistency.ALL
		default:
			return nil, fmt.Errorf("invalid consistency level: %s", parts[queryArgCount(query.Type)])
		}
	}

	return query, nil
}


func queryArgCount(t QueryType) int {
	switch t {
	case PUT:
		return 3
	case GET, DELETE:
		return 2
	default:
		return 0
	}
}


func (qp *QueryProcessor) ExecuteLocal(ctx context.Context, query *Query) (string, error) {
	switch query.Type {
	case PUT:
		err := qp.coordinator.Write(ctx, query.Key, query.Value, query.ConsistencyLevel)
		if err != nil {
			return "", fmt.Errorf("failed to execute PUT: %w", err)
		}
		return "OK", nil
	case GET:
		value, err := qp.coordinator.Read(ctx, query.Key, query.ConsistencyLevel)
		if err != nil {
			return "", fmt.Errorf("failed to execute GET: %w", err)
		}
		return value, nil
	case DELETE:
		err := qp.coordinator.Write(ctx, query.Key, "__TOMBSTONE__", query.ConsistencyLevel)
		if err != nil {
			return "", fmt.Errorf("failed to execute DELETE: %w", err)
		}
		return "OK", nil
	default:
		return "", fmt.Errorf("unsupported query type: %v", query.Type)
	}
}


func (qp *QueryProcessor) ExecuteDistributed(ctx context.Context, query *Query) (string, error) {
	// Get nodes responsible for the key
	nodes, err := qp.partitioner.GetPartitionNodes(query.Key)
	if err != nil {
		return "", fmt.Errorf("failed to get partition nodes: %w", err)
	}

	// If the local node is one of the responsible nodes, use the coordinator directly
	localNodeID := qp.coordinator.GetLocalNodeId()
	for _, node := range nodes {
		if node.ID == localNodeID {
			return qp.ExecuteLocal(ctx, query)
		}
	}


	primaryNode := nodes[0]
	switch query.Type {
	case PUT:
		err := qp.nodeClient.Put(ctx, primaryNode, query.Key, query.Value)
		if err != nil {
			return "", fmt.Errorf("failed to forward PUT to node %s: %w", primaryNode.ID, err)
		}
		return "OK", nil
	case GET:
		value, err := qp.nodeClient.Get(ctx, primaryNode, query.Key)
		if err != nil {
			return "", fmt.Errorf("failed to forward GET to node %s: %w", primaryNode.ID, err)
		}
		return value, nil
	case DELETE:
		err := qp.nodeClient.Put(ctx, primaryNode, query.Key, "__TOMBSTONE__")
		if err != nil {
			return "", fmt.Errorf("failed to forward DELETE to node %s: %w", primaryNode.ID, err)
		}
		return "OK", nil
	default:
		return "", fmt.Errorf("unsupported query type: %v", query.Type)
	}
}


func (qp *QueryProcessor) Execute(ctx context.Context, input string) (string, error) {
	query, err := qp.ParseQuery(input)
	if err != nil {
		return "", fmt.Errorf("failed to parse query: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, qp.timeout)
	defer cancel()

	return qp.ExecuteDistributed(ctx, query)
}