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
	"k8s.io/klog/v2"
)

const (
	// streamPollInterval is how often we poll for new stream records when all
	// shard iterators are exhausted.
	streamPollInterval = 2 * time.Second
	// streamRetryDelay is the backoff applied when a transient error occurs on
	// a shard iterator. The shard is retried after this delay rather than being
	// permanently dropped.
	streamRetryDelay = 5 * time.Second
	// shardDiscoveryInterval is how often we re-call DescribeStream to pick up
	// newly created child shards that appear after DynamoDB shard rotation.
	shardDiscoveryInterval = 30 * time.Second
)

// shardState tracks per-shard polling state so that we can recover expired
// iterators and discover child shards without restarting from TRIM_HORIZON.
type shardState struct {
	shardID    string
	iter       string // current shard iterator; empty means needs refresh
	lastSeqNum string // last successfully processed sequence number; empty = none yet
	errorCount int    // consecutive errors; used for backoff decisions
}

// dynamoDBStreamWatcher implements watch.Interface by tailing a DynamoDB
// Stream on a single table. It discovers the stream ARN from the table
// description, opens all TRIM_HORIZON shard iterators, and delivers
// INSERT / MODIFY / REMOVE events to the result channel.
//
// Child shard discovery: DynamoDB rotates shards over time. When a parent
// shard closes, one or more child shards are created. This watcher
// periodically re-calls DescribeStream to discover child shards so that
// events written after a rotation are not silently missed.
//
// Expired iterator recovery: DynamoDB iterator tokens expire after ~15 minutes
// of inactivity. When an ExpiredIteratorException is returned, the watcher
// re-obtains an iterator using AFTER_SEQUENCE_NUMBER (the last successfully
// processed sequence number) instead of dropping the shard.
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
	klog.V(4).InfoS("stream watcher starting", "table", tableName)
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
			klog.ErrorS(err, "stream watcher failed to get stream ARN", "table", tableName)
			w.sendError(ctx, err)
		}
		return
	}
	if streamARN == "" {
		// Streams not enabled — nothing to watch.
		klog.V(2).InfoS("stream watcher: streams not enabled on table, watch is a no-op", "table", tableName)
		return
	}
	klog.V(4).InfoS("stream watcher connected", "table", tableName, "streamARN", streamARN)

	// Discover all current shards and open TRIM_HORIZON iterators for each.
	shards, err := discoverShards(ctx, streamsClient, streamARN, nil)
	if err != nil {
		if ctx.Err() == nil {
			klog.ErrorS(err, "stream watcher failed to discover shards", "table", tableName)
			w.sendError(ctx, err)
		}
		return
	}
	klog.V(4).InfoS("stream watcher opened shard iterators", "table", tableName, "shards", len(shards))

	lastDiscovery := time.Now()

	// Poll all shard iterators in a round-robin loop.
	for {
		if ctx.Err() != nil {
			return
		}

		// Periodically re-run DescribeStream to find child shards created by
		// DynamoDB shard rotation. We pass the set of already-known shard IDs
		// so that discoverShards only initialises new ones.
		if time.Since(lastDiscovery) >= shardDiscoveryInterval {
			newShards, err := discoverShards(ctx, streamsClient, streamARN, shards)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				klog.V(2).InfoS("stream watcher failed to re-discover shards, will retry", "table", tableName, "err", err)
				// Non-fatal: keep using existing shards; try again next interval.
			} else {
				added := len(newShards) - len(shards)
				if added > 0 {
					klog.V(2).InfoS("stream watcher discovered new shards", "table", tableName, "newShards", added)
				}
				shards = newShards
			}
			lastDiscovery = time.Now()
		}

		if len(shards) == 0 {
			// No shards yet (e.g. brand-new table). Wait then re-discover.
			select {
			case <-ctx.Done():
				return
			case <-time.After(streamPollInterval):
				lastDiscovery = time.Time{} // force rediscovery next iteration
			}
			continue
		}

		anyRecords := false
		for i := range shards {
			s := &shards[i]

			// Refresh the iterator if needed (expired or never obtained).
			if s.iter == "" {
				refreshed, err := refreshShardIterator(ctx, streamsClient, streamARN, s)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					klog.V(2).InfoS("stream watcher failed to refresh shard iterator, will retry", "table", tableName, "shardID", s.shardID, "err", err)
					s.errorCount++
					continue
				}
				if !refreshed {
					// Shard is closed and fully consumed; it will be pruned below.
					continue
				}
			}

			records, nextIter, err := getRecords(ctx, streamsClient, s.iter)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				klog.V(2).InfoS("stream watcher error reading shard, will retry after delay", "table", tableName, "shardID", s.shardID, "err", err)
				s.errorCount++
				// Clear the iterator so it is refreshed on the next pass.
				s.iter = ""
				// Apply retry delay to avoid hammering AWS on sustained errors.
				select {
				case <-ctx.Done():
					return
				case <-time.After(streamRetryDelay):
				}
				continue
			}
			s.errorCount = 0

			for _, rec := range records {
				if rec.Dynamodb == nil {
					continue
				}
				// Track the sequence number for iterator recovery.
				if rec.Dynamodb.SequenceNumber != nil {
					s.lastSeqNum = *rec.Dynamodb.SequenceNumber
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
					klog.V(4).InfoS("stream watcher skipping unconvertible record", "table", tableName, "eventName", rec.EventName, "err", err)
					continue
				}
				if accessor, ok := obj.(metav1.ObjectMetaAccessor); ok {
					klog.V(2).InfoS("stream watcher delivering event", "table", tableName, "eventType", eventType, "documentID", accessor.GetObjectMeta().GetName())
				}
				select {
				case w.resultCh <- watch.Event{Type: eventType, Object: obj}:
				case <-ctx.Done():
					return
				}
				anyRecords = true
			}

			// Update the iterator. An empty nextIter means the shard is closed
			// (all records consumed). We set iter="" so it will be refreshed;
			// refreshShardIterator will return false for a closed shard, which
			// causes it to be pruned from the active list below.
			s.iter = nextIter
		}

		// Prune shards that are closed and fully consumed (iter stays "").
		// A closed shard has no nextIter AND we would fail to get a new
		// iterator for it (refreshShardIterator returns false). We identify
		// these by letting them go through a refresh attempt and marking them
		// with a sentinel. To keep the loop simple we just try once each pass;
		// shards that errored are kept (errorCount > 0, iter == "").
		var active []shardState
		for _, s := range shards {
			if s.iter == "" && s.errorCount == 0 {
				// Shard exhausted and no error — attempt a refresh to confirm
				// it is truly closed. If it cannot be refreshed, drop it.
				refreshed, err := refreshShardIterator(ctx, streamsClient, streamARN, &s)
				if err != nil || !refreshed {
					if ctx.Err() != nil {
						return
					}
					klog.V(4).InfoS("stream watcher shard closed, removing from poll loop", "table", tableName, "shardID", s.shardID)
					// Trigger an immediate child shard discovery so we pick up
					// any children that were created when this parent closed.
					lastDiscovery = time.Time{}
					continue // drop this shard
				}
			}
			active = append(active, s)
		}
		shards = active

		if !anyRecords {
			// No records this round; pause before polling again.
			select {
			case <-ctx.Done():
				return
			case <-time.After(streamPollInterval):
			}
		} else {
			// Brief pause to avoid hot-looping when records keep arriving.
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// discoverShards calls DescribeStream and returns a shardState slice that
// contains all known shards. Shards already present in existing (matched by
// shardID) are preserved as-is; new shards get a fresh TRIM_HORIZON iterator.
// existing may be nil for the initial discovery.
func discoverShards(ctx context.Context, streamsClient *dynamodbstreams.Client, streamARN string, existing []shardState) ([]shardState, error) {
	// Build a lookup of already-tracked shard IDs so we don't reset their state.
	known := make(map[string]shardState, len(existing))
	for _, s := range existing {
		known[s.shardID] = s
	}

	var result []shardState
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
			id := *shard.ShardId
			if s, alreadyKnown := known[id]; alreadyKnown {
				// Preserve existing iterator and sequence state.
				result = append(result, s)
				continue
			}
			// New shard — open a TRIM_HORIZON iterator.
			iterOut, err := streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamARN),
				ShardId:           aws.String(id),
				ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
			})
			if err != nil {
				// Skip inaccessible shards but log at a visible level.
				klog.V(2).InfoS("stream watcher failed to get iterator for shard, skipping", "shardID", id, "err", err)
				continue
			}
			iter := ""
			if iterOut.ShardIterator != nil {
				iter = *iterOut.ShardIterator
			}
			result = append(result, shardState{
				shardID: id,
				iter:    iter,
			})
		}
		if out.StreamDescription.LastEvaluatedShardId == nil {
			break
		}
		lastShardID = out.StreamDescription.LastEvaluatedShardId
	}
	return result, nil
}

// refreshShardIterator attempts to obtain a new iterator for a shard whose
// current iterator has been cleared (expired or end-of-shard). It uses
// AFTER_SEQUENCE_NUMBER if a sequence number has been seen, otherwise
// TRIM_HORIZON. Returns (true, nil) if a new iterator was obtained,
// (false, nil) if the shard is fully closed, or (false, err) on error.
func refreshShardIterator(ctx context.Context, streamsClient *dynamodbstreams.Client, streamARN string, s *shardState) (bool, error) {
	input := &dynamodbstreams.GetShardIteratorInput{
		StreamArn: aws.String(streamARN),
		ShardId:   aws.String(s.shardID),
	}
	if s.lastSeqNum != "" {
		input.ShardIteratorType = streamtypes.ShardIteratorTypeAfterSequenceNumber
		input.SequenceNumber = aws.String(s.lastSeqNum)
	} else {
		input.ShardIteratorType = streamtypes.ShardIteratorTypeTrimHorizon
	}
	out, err := streamsClient.GetShardIterator(ctx, input)
	if err != nil {
		return false, err
	}
	if out.ShardIterator == nil {
		// Shard is closed and fully consumed.
		return false, nil
	}
	s.iter = *out.ShardIterator
	return true, nil
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
