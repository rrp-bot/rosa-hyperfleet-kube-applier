package informers

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	// streamPollInterval is how often we poll for new stream records when no
	// shard iterator is exhausted.
	streamPollInterval = 2 * time.Second
	// streamRetryDelay is the backoff when an error occurs.
	streamRetryDelay = 5 * time.Second
)

// dynamoDBStreamWatcher implements watch.Interface by tailing a DynamoDB
// Stream on a single table. It discovers the stream ARN from the table
// description, opens all TRIM_HORIZON shard iterators, and delivers
// INSERT / MODIFY / REMOVE events to the result channel.
type dynamoDBStreamWatcher struct {
	resultCh chan watch.Event
	done     chan struct{}
	cancel   context.CancelFunc
}

func newDynamoDBStreamWatcher(
	ctx context.Context,
	dbClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	tableName string,
	convertFn func(map[string]streamtypes.AttributeValue) (runtime.Object, error),
) *dynamoDBStreamWatcher {
	ctx, cancel := context.WithCancel(ctx)
	w := &dynamoDBStreamWatcher{
		resultCh: make(chan watch.Event, 100),
		done:     make(chan struct{}),
		cancel:   cancel,
	}
	go w.run(ctx, dbClient, streamsClient, tableName, convertFn)
	return w
}

func (w *dynamoDBStreamWatcher) run(
	ctx context.Context,
	dbClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	tableName string,
	convertFn func(map[string]streamtypes.AttributeValue) (runtime.Object, error),
) {
	defer close(w.done)
	defer close(w.resultCh)

	// Get the stream ARN from the table description.
	streamARN, err := getTableStreamARN(ctx, dbClient, tableName)
	if err != nil {
		if ctx.Err() == nil {
			w.sendError(ctx, err)
		}
		return
	}
	if streamARN == "" {
		// Streams not enabled — nothing to watch.
		return
	}

	// Get all shards for the stream.
	shardIters, err := getShardIterators(ctx, streamsClient, streamARN)
	if err != nil {
		if ctx.Err() == nil {
			w.sendError(ctx, err)
		}
		return
	}

	// Poll all shard iterators in a round-robin loop.
	for {
		if ctx.Err() != nil {
			return
		}
		if len(shardIters) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(streamPollInterval):
				// Re-discover shards (new shards may appear after a reshard).
				shardIters, err = getShardIterators(ctx, streamsClient, streamARN)
				if err != nil {
					if ctx.Err() == nil {
						w.sendError(ctx, err)
					}
					return
				}
				continue
			}
		}

		var nextIters []string
		for _, iter := range shardIters {
			records, nextIter, err := getRecords(ctx, streamsClient, iter)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				// Iterator expired or other transient error — skip this shard.
				continue
			}
			for _, rec := range records {
				if rec.Dynamodb == nil {
					continue
				}
				var image map[string]streamtypes.AttributeValue
				var eventType watch.EventType
				switch rec.EventName {
				case streamtypes.OperationTypeInsert:
					eventType = watch.Added
					image = rec.Dynamodb.NewImage
				case streamtypes.OperationTypeModify:
					eventType = watch.Modified
					image = rec.Dynamodb.NewImage
				case streamtypes.OperationTypeRemove:
					eventType = watch.Deleted
					image = rec.Dynamodb.OldImage
				default:
					continue
				}
				if image == nil {
					continue
				}
				obj, err := convertFn(image)
				if err != nil {
					continue
				}
				select {
				case w.resultCh <- watch.Event{Type: eventType, Object: obj}:
				case <-ctx.Done():
					return
				}
			}
			if nextIter != "" {
				nextIters = append(nextIters, nextIter)
			}
		}
		shardIters = nextIters

		if len(shardIters) == 0 {
			// All iterators consumed; wait before re-discovering.
			select {
			case <-ctx.Done():
				return
			case <-time.After(streamPollInterval):
			}
		} else {
			// Brief pause to avoid hot-looping.
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// getTableStreamARN retrieves the latest stream ARN for a table (empty string
// if Streams are not enabled).
func getTableStreamARN(ctx context.Context, dbClient *dynamodb.Client, tableName string) (string, error) {
	out, err := dbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return "", err
	}
	if out.Table.LatestStreamArn == nil {
		return "", nil
	}
	return *out.Table.LatestStreamArn, nil
}

// getShardIterators lists all shards for the given stream and returns a
// TRIM_HORIZON iterator for each shard.
func getShardIterators(ctx context.Context, streamsClient *dynamodbstreams.Client, streamARN string) ([]string, error) {
	var iters []string
	var lastShardID *string
	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(streamARN),
			ExclusiveStartShardId: lastShardID,
		}
		out, err := streamsClient.DescribeStream(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, shard := range out.StreamDescription.Shards {
			if shard.ShardId == nil {
				continue
			}
			iterOut, err := streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamARN),
				ShardId:           shard.ShardId,
				ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
			})
			if err != nil {
				// Skip inaccessible shards.
				continue
			}
			if iterOut.ShardIterator != nil {
				iters = append(iters, *iterOut.ShardIterator)
			}
		}
		if out.StreamDescription.LastEvaluatedShardId == nil {
			break
		}
		lastShardID = out.StreamDescription.LastEvaluatedShardId
	}
	return iters, nil
}

// getRecords calls GetRecords for one shard iterator and returns the records
// plus the next iterator (empty string when the shard is exhausted).
func getRecords(ctx context.Context, streamsClient *dynamodbstreams.Client, shardIterator string) ([]streamtypes.Record, string, error) {
	out, err := streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String(shardIterator),
	})
	if err != nil {
		return nil, "", err
	}
	nextIter := ""
	if out.NextShardIterator != nil {
		nextIter = *out.NextShardIterator
	}
	return out.Records, nextIter, nil
}

func (w *dynamoDBStreamWatcher) sendError(ctx context.Context, err error) {
	event := watch.Event{
		Type: watch.Error,
		Object: &metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusGone,
			Reason:  metav1.StatusReasonExpired,
			Message: err.Error(),
		},
	}
	select {
	case w.resultCh <- event:
	case <-ctx.Done():
	}
}

func (w *dynamoDBStreamWatcher) Stop() {
	w.cancel()
}

func (w *dynamoDBStreamWatcher) ResultChan() <-chan watch.Event {
	return w.resultCh
}
