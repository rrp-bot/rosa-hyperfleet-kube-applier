package database

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
)

// Table name suffixes appended to the specs/status prefix supplied at startup.
const (
	TableSuffixApplyDesires = "-applydesires"
	TableSuffixReadDesires  = "-readdesires"
)

// SpecPollReader is the interface required by the timestamp-polling informers.
// It extends the basic List with ListSince for efficient change detection, and
// is kept separate from SpecReader so the Kubernetes-style interface is not
// polluted with a non-Kubernetes concept.
type SpecPollReader[T any] interface {
	List(ctx context.Context) ([]*T, error)
	ListSince(ctx context.Context, since time.Time) ([]*T, error)
}

// NewApplyDesireSpecReader returns a SpecPollReader for the apply-desires specs
// table. It is used by the informers package to construct the poll watcher.
func NewApplyDesireSpecReader(client *dynamodb.Client, table string) SpecPollReader[kubeapplier.ApplyDesire] {
	return &dynamoDBSpecReader[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]{
		client: client,
		table:  table,
	}
}

// NewReadDesireSpecReader returns a SpecPollReader for the read-desires specs
// table. It is used by the informers package to construct the poll watcher.
func NewReadDesireSpecReader(client *dynamodb.Client, table string) SpecPollReader[kubeapplier.ReadDesire] {
	return &dynamoDBSpecReader[kubeapplier.ReadDesire, *kubeapplier.ReadDesire]{
		client: client,
		table:  table,
	}
}

// dynamoDBKubeApplierDBClient wraps two DynamoDB clients (one for specs tables,
// one for status tables) and derives the table names from the two prefixes.
type dynamoDBKubeApplierDBClient struct {
	specsClient  *dynamodb.Client
	statusClient *dynamodb.Client
	specsPrefix  string
	statusPrefix string
}

// NewDynamoDBKubeApplierDBClient returns a KubeApplierDBClient backed by two
// DynamoDB clients: specsClient for read-only spec access and statusClient for
// read-write status access. specsPrefix and statusPrefix are the table name
// prefixes; the full table names are prefix + suffix (e.g.
// "mc-dev-specs-applydesires").
func NewDynamoDBKubeApplierDBClient(specsClient, statusClient *dynamodb.Client, specsPrefix, statusPrefix string) KubeApplierDBClient {
	return &dynamoDBKubeApplierDBClient{
		specsClient:  specsClient,
		statusClient: statusClient,
		specsPrefix:  specsPrefix,
		statusPrefix: statusPrefix,
	}
}

func (c *dynamoDBKubeApplierDBClient) ApplyDesireSpecs() SpecReader[kubeapplier.ApplyDesire] {
	return &dynamoDBSpecReader[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]{
		client: c.specsClient,
		table:  c.specsPrefix + TableSuffixApplyDesires,
	}
}

func (c *dynamoDBKubeApplierDBClient) ReadDesireSpecs() SpecReader[kubeapplier.ReadDesire] {
	return &dynamoDBSpecReader[kubeapplier.ReadDesire, *kubeapplier.ReadDesire]{
		client: c.specsClient,
		table:  c.specsPrefix + TableSuffixReadDesires,
	}
}

func (c *dynamoDBKubeApplierDBClient) ApplyDesireStatus() ResourceCRUD[kubeapplier.ApplyDesire] {
	return &dynamoDBDesireCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]{
		client: c.statusClient,
		table:  c.statusPrefix + TableSuffixApplyDesires,
	}
}

func (c *dynamoDBKubeApplierDBClient) ReadDesireStatus() ResourceCRUD[kubeapplier.ReadDesire] {
	return &dynamoDBDesireCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire]{
		client: c.statusClient,
		table:  c.statusPrefix + TableSuffixReadDesires,
	}
}

// Close is a no-op: the AWS SDK DynamoDB client has no connection to close.
func (c *dynamoDBKubeApplierDBClient) Close() error { return nil }
