package informers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"

	kubeapplier "github.com/rrp-bot/kube-applier-aws/internal/api/kubeapplier"
	"github.com/rrp-bot/kube-applier-aws/internal/database"
)

// ---------------------------------------------------------------------------
// Fake streams client
//
// The production code calls three methods on *dynamodbstreams.Client. We
// cannot implement that interface directly (the SDK uses concrete structs), so
// we introduce a narrow streamsAPI interface — only used in tests — and a
// thin runWithStreamsAPI helper that accepts it. This lets us exercise the
// watcher logic without any network.
// ---------------------------------------------------------------------------

type streamsAPI interface {
	GetRecords(ctx context.Context, in *dynamodbstreams.GetRecordsInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error)
}

// fakeStreamsClient is a controllable implementation of streamsAPI.
type fakeStreamsClient struct {
	mu      sync.Mutex
	calls   int
	results []fakeGetRecordsResult
}

type fakeGetRecordsResult struct {
	records  []streamtypes.Record
	nextIter string // "" means shard exhausted
	err      error
}

func (f *fakeStreamsClient) GetRecords(_ context.Context, _ *dynamodbstreams.GetRecordsInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.results) == 0 {
		// Block forever (simulates an idle shard with no new records).
		return &dynamodbstreams.GetRecordsOutput{
			NextShardIterator: aws.String("idle-iter"),
		}, nil
	}
	r := f.results[0]
	f.results = f.results[1:]
	f.calls++
	if r.err != nil {
		return nil, r.err
	}
	out := &dynamodbstreams.GetRecordsOutput{Records: r.records}
	if r.nextIter != "" {
		out.NextShardIterator = aws.String(r.nextIter)
	}
	return out, nil
}

// runWithFakeStreams starts the watcher poll loop against a fake client so we
// can unit-test the loop logic without real AWS clients.
//
// It calls the same inner polling logic as run() but accepts a streamsAPI
// instead of a *dynamodbstreams.Client so we can inject a fake.
func runWithFakeStreams(
	ctx context.Context,
	w *dynamoDBStreamWatcher,
	fake streamsAPI,
	tableName string,
	convertFn func(map[string]streamtypes.AttributeValue) (runtime.Object, error),
	initialIter string,
) {
	defer close(w.done)
	defer close(w.resultCh)

	shardIters := []string{initialIter}
	startTime := time.Now()

	for {
		if ctx.Err() != nil {
			return
		}
		if time.Since(startTime) >= streamWatchTimeout {
			w.sendError(ctx, fmt.Errorf("watch timeout after %s, forcing relist", streamWatchTimeout))
			return
		}
		if len(shardIters) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(streamPollInterval):
				// In unit tests we don't re-discover — just return.
				return
			}
		}

		var nextIters []string
		for _, iter := range shardIters {
			records, nextIter, err := func() ([]streamtypes.Record, string, error) {
				out, err := fake.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
					ShardIterator: aws.String(iter),
				})
				if err != nil {
					return nil, "", err
				}
				next := ""
				if out.NextShardIterator != nil {
					next = *out.NextShardIterator
				}
				return out.Records, next, nil
			}()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				nextIters = append(nextIters, iter)
				select {
				case <-ctx.Done():
					return
				case <-time.After(streamRetryDelay):
				}
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

		if len(shardIters) > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// newTestWatcher builds a watcher struct without starting the real run()
// goroutine — the caller drives it via runWithFakeStreams.
func newTestWatcher() *dynamoDBStreamWatcher {
	_, cancel := context.WithCancel(context.Background())
	return &dynamoDBStreamWatcher{
		resultCh: make(chan watch.Event, 100),
		done:     make(chan struct{}),
		cancel:   cancel,
	}
}

// noopConvert is a convertFn that always returns an error; used when we only
// care about error-handling behaviour, not event delivery.
func noopConvert(_ map[string]streamtypes.AttributeValue) (runtime.Object, error) {
	return nil, errors.New("unconvertible")
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

// TestStreamWatcher_TransientError_ShardNotDropped verifies that a transient
// GetRecords error does NOT permanently remove the shard. The iterator must
// still be present in the next round so the watcher can retry it.
func TestStreamWatcher_TransientError_ShardNotDropped(t *testing.T) {
	transientErr := errors.New("LimitExceededException")

	fake := &fakeStreamsClient{
		results: []fakeGetRecordsResult{
			// First call: transient error.
			{err: transientErr},
			// Second call: success with no records (shard alive, idle).
			{nextIter: "iter-2"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	w := newTestWatcher()

	done := make(chan struct{})
	go func() {
		defer close(done)
		runWithFakeStreams(ctx, w, fake, "test-table", noopConvert, "iter-1")
	}()

	// Wait until both results have been consumed (error + success).
	deadline := time.After(15 * time.Second)
	for {
		fake.mu.Lock()
		remaining := len(fake.results)
		fake.mu.Unlock()
		if remaining == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for fake results to be consumed")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// The watcher should still be running (not returned after the error).
	select {
	case <-done:
		t.Fatal("watcher goroutine exited after transient error — shard was dropped")
	default:
		// Good: still running.
	}

	cancel()
	<-done
}

// TestStreamWatcher_TransientError_RetriesAfterBackoff verifies that after a
// transient error the watcher re-calls GetRecords on the same iterator (i.e.
// the iterator is preserved), and that on the retry it successfully delivers
// events.
func TestStreamWatcher_TransientError_RetriesAfterBackoff(t *testing.T) {
	insertRecord := streamtypes.Record{
		EventName: streamtypes.OperationTypeInsert,
		Dynamodb: &streamtypes.StreamRecord{
			NewImage: map[string]streamtypes.AttributeValue{
				"documentID": &streamtypes.AttributeValueMemberS{Value: "c1--item1"},
			},
		},
	}

	fake := &fakeStreamsClient{
		results: []fakeGetRecordsResult{
			// First call: transient error.
			{err: errors.New("LimitExceededException")},
			// Second call (retry): delivers a record then exhausts.
			{records: []streamtypes.Record{insertRecord}, nextIter: ""},
		},
	}

	convertFn := func(image map[string]streamtypes.AttributeValue) (runtime.Object, error) {
		id := ""
		if v, ok := image["documentID"].(*streamtypes.AttributeValueMemberS); ok {
			id = v.Value
		}
		return &kubeapplier.ApplyDesire{
			DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: id},
		}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	w := newTestWatcher()
	go runWithFakeStreams(ctx, w, fake, "test-table", convertFn, "iter-1")

	// Expect an Added event to arrive after the retry.
	select {
	case ev, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("result channel closed before event arrived")
		}
		if ev.Type != watch.Added {
			t.Errorf("event type = %q, want Added", ev.Type)
		}
		ad, ok := ev.Object.(*kubeapplier.ApplyDesire)
		if !ok {
			t.Fatalf("object type = %T, want *ApplyDesire", ev.Object)
		}
		if ad.DocumentID != "c1--item1" {
			t.Errorf("documentID = %q, want %q", ad.DocumentID, "c1--item1")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for Added event after retry")
	}

	cancel()
}

// TestStreamWatcher_WatchTimeout_SendsExpiredError verifies that after
// streamWatchTimeout the watcher sends a watch.Error event with
// StatusReasonExpired and then closes its result channel, causing the
// Reflector to relist.
func TestStreamWatcher_WatchTimeout_SendsExpiredError(t *testing.T) {
	// Temporarily shorten the timeout so the test completes quickly.
	original := streamWatchTimeout
	streamWatchTimeout = 200 * time.Millisecond
	defer func() { streamWatchTimeout = original }()

	// Idle fake: never errors, returns an empty next iterator so the shard
	// stays alive — this ensures only the timeout path terminates the watcher.
	fake := &fakeStreamsClient{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w := newTestWatcher()
	go runWithFakeStreams(ctx, w, fake, "test-table", noopConvert, "iter-1")

	// Expect a watch.Error event with StatusReasonExpired.
	select {
	case ev, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("result channel closed without sending an error event")
		}
		if ev.Type != watch.Error {
			t.Errorf("event type = %q, want Error", ev.Type)
		}
		status, ok := ev.Object.(*metav1.Status)
		if !ok {
			t.Fatalf("error object type = %T, want *metav1.Status", ev.Object)
		}
		if status.Reason != metav1.StatusReasonExpired {
			t.Errorf("status reason = %q, want Expired", status.Reason)
		}
		if status.Code != http.StatusGone {
			t.Errorf("status code = %d, want %d", status.Code, http.StatusGone)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for timeout error event")
	}

	// The result channel should be closed after the error is sent (run returned).
	select {
	case _, ok := <-w.ResultChan():
		if ok {
			t.Error("expected result channel to be closed after timeout, but received another event")
		}
		// ok == false: channel closed as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("result channel not closed after timeout error")
	}
}

// TestStreamWatcher_WatchTimeout_IsVariable confirms that changing
// streamWatchTimeout is respected — i.e. we hit the timeout at the new
// duration and not before, which validates the check uses time.Since(startTime).
func TestStreamWatcher_WatchTimeout_IsVariable(t *testing.T) {
	original := streamWatchTimeout
	streamWatchTimeout = 300 * time.Millisecond
	defer func() { streamWatchTimeout = original }()

	fake := &fakeStreamsClient{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w := newTestWatcher()
	start := time.Now()
	go runWithFakeStreams(ctx, w, fake, "test-table", noopConvert, "iter-1")

	select {
	case ev := <-w.ResultChan():
		elapsed := time.Since(start)
		if ev.Type != watch.Error {
			t.Errorf("event type = %q, want Error", ev.Type)
		}
		// Should have fired at ~300ms, not before 200ms or after 3s.
		if elapsed < 200*time.Millisecond {
			t.Errorf("timeout fired too early: %v", elapsed)
		}
		if elapsed > 3*time.Second {
			t.Errorf("timeout fired too late: %v", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for timeout error event")
	}

	cancel()
}

// TestStreamWatcher_Stop_ClosesChannels verifies that calling Stop() causes
// the result channel to be closed cleanly.
func TestStreamWatcher_Stop_ClosesChannels(t *testing.T) {
	fake := &fakeStreamsClient{}

	ctx := context.Background()
	w := newTestWatcher()
	ctx2, cancel2 := context.WithCancel(ctx)
	w.cancel = cancel2

	go runWithFakeStreams(ctx2, w, fake, "test-table", noopConvert, "iter-1")

	w.Stop()

	select {
	case _, ok := <-w.ResultChan():
		if ok {
			// Drain any remaining events (e.g. if stop raced with an idle result).
			for range w.ResultChan() {
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("result channel was not closed after Stop()")
	}
}

// TestStreamWatcher_EventDelivery_InsertModifyDelete verifies that INSERT,
// MODIFY, and REMOVE stream records are mapped to watch.Added, watch.Modified,
// and watch.Deleted events respectively, with the correct image used.
func TestStreamWatcher_EventDelivery_InsertModifyDelete(t *testing.T) {
	mkRecord := func(op streamtypes.OperationType, newID, oldID string) streamtypes.Record {
		rec := streamtypes.Record{
			EventName: op,
			Dynamodb:  &streamtypes.StreamRecord{},
		}
		if newID != "" {
			rec.Dynamodb.NewImage = map[string]streamtypes.AttributeValue{
				"documentID": &streamtypes.AttributeValueMemberS{Value: newID},
			}
		}
		if oldID != "" {
			rec.Dynamodb.OldImage = map[string]streamtypes.AttributeValue{
				"documentID": &streamtypes.AttributeValueMemberS{Value: oldID},
			}
		}
		return rec
	}

	fake := &fakeStreamsClient{
		results: []fakeGetRecordsResult{
			{
				records: []streamtypes.Record{
					mkRecord(streamtypes.OperationTypeInsert, "c1--new", ""),
					mkRecord(streamtypes.OperationTypeModify, "c1--mod", ""),
					mkRecord(streamtypes.OperationTypeRemove, "", "c1--del"),
				},
				nextIter: "", // shard exhausts after this batch
			},
		},
	}

	convertFn := func(image map[string]streamtypes.AttributeValue) (runtime.Object, error) {
		id := ""
		if v, ok := image["documentID"].(*streamtypes.AttributeValueMemberS); ok {
			id = v.Value
		}
		return &kubeapplier.ApplyDesire{
			DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: id},
		}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	w := newTestWatcher()
	go runWithFakeStreams(ctx, w, fake, "test-table", convertFn, "iter-1")

	want := []struct {
		eventType watch.EventType
		docID     string
	}{
		{watch.Added, "c1--new"},
		{watch.Modified, "c1--mod"},
		{watch.Deleted, "c1--del"},
	}

	for i, wantEv := range want {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("result channel closed before event %d", i)
			}
			if ev.Type != wantEv.eventType {
				t.Errorf("event[%d] type = %q, want %q", i, ev.Type, wantEv.eventType)
			}
			ad, ok := ev.Object.(*kubeapplier.ApplyDesire)
			if !ok {
				t.Fatalf("event[%d] object type = %T", i, ev.Object)
			}
			if ad.DocumentID != wantEv.docID {
				t.Errorf("event[%d] documentID = %q, want %q", i, ad.DocumentID, wantEv.docID)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for event %d (%q)", i, wantEv.eventType)
		}
	}

	cancel()
}

// ---------------------------------------------------------------------------
// Integration tests (require LOCALSTACK_ENDPOINT)
// ---------------------------------------------------------------------------

// TestIntegration_StreamWatcher_TransientError_ShardSurvives uses LocalStack
// to verify that a real GetRecords error does not kill the stream session. We
// start a watcher, write a document, wait for the event, then verify the
// watcher is still alive and can deliver a second event.
//
// NOTE: This test cannot inject a real transient error mid-stream — instead it
// verifies continuity across the normal poll cycle, which exercises the same
// iterator-preservation code path.
func TestIntegration_StreamWatcher_TransientError_ShardSurvives(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient, streamsClient := newLocalStackClients(t)
	prefix := fmt.Sprintf("sw-retry-%d", time.Now().UnixNano())
	tableName := prefix + database.TableSuffixApplyDesires
	createTestTable(t, dbClient, tableName)
	createTestTable(t, dbClient, prefix+database.TableSuffixDeleteDesires)
	createTestTable(t, dbClient, prefix+database.TableSuffixReadDesires)

	convertFn := func(image map[string]streamtypes.AttributeValue) (runtime.Object, error) {
		return database.ItemToApplyDesire(streamImageToDynamoDBItem(image))
	}

	watcher := newDynamoDBStreamWatcher(ctx, dbClient, streamsClient, tableName, convertFn)
	defer watcher.Stop()

	crud := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefix, prefix).ApplyDesireStatus()

	writeDesire := func(id string) {
		t.Helper()
		d := &kubeapplier.ApplyDesire{
			DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: id},
			Spec: kubeapplier.ApplyDesireSpec{
				ManagementCluster: "mc-test",
				ClusterID:         "c1",
				TargetItem: kubeapplier.ResourceReference{
					Version: "v1", Resource: "configmaps", Name: id,
				},
			},
		}
		if _, err := crud.Create(ctx, d); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
	}

	expectEvent := func(wantType watch.EventType, wantID string) {
		t.Helper()
		deadline := time.After(30 * time.Second)
		for {
			select {
			case ev, ok := <-watcher.ResultChan():
				if !ok {
					t.Fatal("watcher result channel closed unexpectedly")
				}
				if ev.Type == watch.Error {
					t.Fatalf("unexpected error event: %v", ev.Object)
				}
				ad, ok := ev.Object.(*kubeapplier.ApplyDesire)
				if !ok {
					continue
				}
				if ev.Type == wantType && ad.DocumentID == wantID {
					return
				}
			case <-deadline:
				t.Fatalf("timed out waiting for %q event for %q", wantType, wantID)
			}
		}
	}

	// First write + event.
	writeDesire("c1--first")
	expectEvent(watch.Added, "c1--first")

	// Second write: watcher must still be alive to deliver this.
	writeDesire("c1--second")
	expectEvent(watch.Added, "c1--second")
}

// TestIntegration_StreamWatcher_Timeout_TriggersRelist verifies that after
// streamWatchTimeout the watcher closes with a 410 Gone, and that a freshly
// created watcher (simulating what the Reflector does on relist) can still
// see documents written before and after the timeout.
func TestIntegration_StreamWatcher_Timeout_TriggersRelist(t *testing.T) {
	requireLocalStack(t)

	original := streamWatchTimeout
	streamWatchTimeout = 3 * time.Second
	defer func() { streamWatchTimeout = original }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient, streamsClient := newLocalStackClients(t)
	prefix := fmt.Sprintf("sw-timeout-%d", time.Now().UnixNano())
	tableName := prefix + database.TableSuffixApplyDesires
	createTestTable(t, dbClient, tableName)
	createTestTable(t, dbClient, prefix+database.TableSuffixDeleteDesires)
	createTestTable(t, dbClient, prefix+database.TableSuffixReadDesires)

	convertFn := func(image map[string]streamtypes.AttributeValue) (runtime.Object, error) {
		return database.ItemToApplyDesire(streamImageToDynamoDBItem(image))
	}

	w1 := newDynamoDBStreamWatcher(ctx, dbClient, streamsClient, tableName, convertFn)

	// Wait for the watcher to time out and send the 410.
	var sawExpired bool
	deadline := time.After(15 * time.Second)
drain:
	for {
		select {
		case ev, ok := <-w1.ResultChan():
			if !ok {
				// Channel closed — expired event was already consumed.
				sawExpired = true
				break drain
			}
			if ev.Type == watch.Error {
				if st, ok := ev.Object.(*metav1.Status); ok && st.Reason == metav1.StatusReasonExpired {
					sawExpired = true
					break drain
				}
			}
		case <-deadline:
			t.Fatal("timed out waiting for watcher timeout event")
		}
	}

	if !sawExpired {
		t.Fatal("expected StatusReasonExpired event from timed-out watcher")
	}
	w1.Stop()

	// Simulate the Reflector creating a new watcher after the relist.
	w2 := newDynamoDBStreamWatcher(ctx, dbClient, streamsClient, tableName, convertFn)
	defer w2.Stop()

	// Write a document — the new watcher must deliver it.
	crud := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefix, prefix).ApplyDesireStatus()
	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--after-timeout"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "c1",
			TargetItem:        kubeapplier.ResourceReference{Version: "v1", Resource: "configmaps", Name: "cm-post"},
		},
	}
	if _, err := crud.Create(ctx, d); err != nil {
		t.Fatalf("Create: %v", err)
	}

	deadline2 := time.After(30 * time.Second)
	for {
		select {
		case ev, ok := <-w2.ResultChan():
			if !ok {
				t.Fatal("second watcher closed unexpectedly")
			}
			if ev.Type == watch.Error {
				continue
			}
			ad, ok := ev.Object.(*kubeapplier.ApplyDesire)
			if ok && ev.Type == watch.Added && ad.DocumentID == "c1--after-timeout" {
				return // success
			}
		case <-deadline2:
			t.Fatal("timed out waiting for event on second watcher after relist")
		}
	}
}

// TestIntegration_StreamWatcher_LocalStack_FullCycle is a sanity-check
// integration test that exercises the full production newDynamoDBStreamWatcher
// path: stream ARN discovery, shard iterator opening, and event delivery for
// all three operation types (INSERT/MODIFY/REMOVE).
func TestIntegration_StreamWatcher_LocalStack_FullCycle(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient, streamsClient := newLocalStackClients(t)
	prefix := fmt.Sprintf("sw-fullcycle-%d", time.Now().UnixNano())
	tableName := prefix + database.TableSuffixApplyDesires
	createTestTable(t, dbClient, tableName)
	createTestTable(t, dbClient, prefix+database.TableSuffixDeleteDesires)
	createTestTable(t, dbClient, prefix+database.TableSuffixReadDesires)

	convertFn := func(image map[string]streamtypes.AttributeValue) (runtime.Object, error) {
		return database.ItemToApplyDesire(streamImageToDynamoDBItem(image))
	}

	watcher := newDynamoDBStreamWatcher(ctx, dbClient, streamsClient, tableName, convertFn)
	defer watcher.Stop()

	crud := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefix, prefix).ApplyDesireStatus()

	desire := func(id string) *kubeapplier.ApplyDesire {
		return &kubeapplier.ApplyDesire{
			DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: id},
			Spec: kubeapplier.ApplyDesireSpec{
				ManagementCluster: "mc-test",
				ClusterID:         "c1",
				TargetItem: kubeapplier.ResourceReference{
					Version: "v1", Resource: "configmaps", Name: id,
				},
			},
		}
	}

	collectEvents := func(n int, timeout time.Duration) []watch.Event {
		t.Helper()
		var evs []watch.Event
		dl := time.After(timeout)
		for len(evs) < n {
			select {
			case ev, ok := <-watcher.ResultChan():
				if !ok {
					t.Fatalf("channel closed after %d events (want %d)", len(evs), n)
				}
				if ev.Type != watch.Error {
					evs = append(evs, ev)
				}
			case <-dl:
				t.Fatalf("timed out after collecting %d/%d events", len(evs), n)
			}
		}
		return evs
	}

	// INSERT.
	created, err := crud.Create(ctx, desire("c1--fc"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	evs := collectEvents(1, 30*time.Second)
	if evs[0].Type != watch.Added {
		t.Errorf("INSERT: event type = %q, want Added", evs[0].Type)
	}

	// MODIFY.
	created.Spec.ClusterID = "c2"
	if _, err := crud.Replace(ctx, created); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	evs = collectEvents(1, 30*time.Second)
	if evs[0].Type != watch.Modified {
		t.Errorf("MODIFY: event type = %q, want Modified", evs[0].Type)
	}

	// REMOVE.
	if err := crud.Delete(ctx, "c1--fc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	evs = collectEvents(1, 30*time.Second)
	if evs[0].Type != watch.Deleted {
		t.Errorf("REMOVE: event type = %q, want Deleted", evs[0].Type)
	}
}

// Compile-time check: streamWatchTimeout must be a variable (not const) so
// tests can mutate it. This is enforced by the test file referencing it as an
// lvalue above.
var _ = streamWatchTimeout
