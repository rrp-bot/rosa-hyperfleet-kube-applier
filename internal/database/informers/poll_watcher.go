package informers

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"
)

const (
	// defaultPollInterval is how often the quick poll queries DynamoDB for
	// recently updated items.
	defaultPollInterval = 15 * time.Second

	// defaultWatchDuration is how long a poll watcher runs before closing,
	// causing the SharedIndexInformer to trigger a full consistent re-list
	// followed by a fresh poll watcher. This is the mechanism that gives the
	// 5-minute unconditional relist guarantee.
	defaultWatchDuration = 5 * time.Minute
)

// sinceReader is the minimal interface the poll watcher requires from the
// database layer. It is satisfied by *dynamoDBSpecReader but is kept as a
// local interface so the watcher can be tested without a real DynamoDB client.
type sinceReader[T any] interface {
	ListSince(ctx context.Context, since time.Time) ([]*T, error)
}

// convertFn converts a typed desire value into a runtime.Object suitable for
// delivery to the SharedIndexInformer event channel.
type convertFn[T any] func(*T) (runtime.Object, error)

// dynamoDBPollWatcher implements watch.Interface. It polls DynamoDB on a fixed
// interval for items whose updateTime is within the lookback window, sending
// Modified events to the result channel. After watchDuration it closes the
// result channel, which causes the SharedIndexInformer to perform a full
// consistent re-list and then call Watch again.
//
// Correctness model:
//   - The quick poll (every pollInterval) surfaces recently changed items with
//     low latency. It uses eventually consistent reads and a lookback window
//     equal to watchDuration to absorb any clock skew or GSI propagation delay.
//   - The full re-list (triggered when this watcher closes) fetches every item
//     with a consistent Scan, guaranteeing that nothing is permanently missed.
//   - Controllers always call GetItem (ConsistentRead=true) before acting, so
//     the Modified events here are purely a notification mechanism.
type dynamoDBPollWatcher[T any] struct {
	resultCh chan watch.Event
	cancel   context.CancelFunc
	done     chan struct{}
}

func newDynamoDBPollWatcher[T any](
	ctx context.Context,
	tableName string,
	reader sinceReader[T],
	convert convertFn[T],
	pollInterval time.Duration,
	watchDuration time.Duration,
) *dynamoDBPollWatcher[T] {
	ctx, cancel := context.WithCancel(ctx)
	w := &dynamoDBPollWatcher[T]{
		resultCh: make(chan watch.Event, 100),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go w.run(ctx, tableName, reader, convert, pollInterval, watchDuration)
	return w
}

func (w *dynamoDBPollWatcher[T]) run(
	ctx context.Context,
	tableName string,
	reader sinceReader[T],
	convert convertFn[T],
	pollInterval time.Duration,
	watchDuration time.Duration,
) {
	defer close(w.done)
	defer close(w.resultCh)

	klog.V(4).InfoS("poll watcher starting", "table", tableName,
		"pollInterval", pollInterval, "watchDuration", watchDuration)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	// After watchDuration the watcher closes, causing the informer to re-list.
	deadline := time.NewTimer(watchDuration)
	defer deadline.Stop()

	for {
		select {
		case <-ctx.Done():
			klog.V(4).InfoS("poll watcher stopping (context cancelled)", "table", tableName)
			return

		case <-deadline.C:
			// Intentional close — the informer will perform a full re-list.
			klog.V(4).InfoS("poll watcher closing to trigger re-list", "table", tableName)
			return

		case <-ticker.C:
			w.poll(ctx, tableName, reader, convert, watchDuration)
		}
	}
}

func (w *dynamoDBPollWatcher[T]) poll(
	ctx context.Context,
	tableName string,
	reader sinceReader[T],
	convert convertFn[T],
	lookback time.Duration,
) {
	since := time.Now().UTC().Add(-lookback)
	items, err := reader.ListSince(ctx, since)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		klog.V(2).InfoS("poll watcher scan error", "table", tableName, "err", err)
		return
	}

	klog.V(5).InfoS("poll watcher scan complete", "table", tableName, "items", len(items))

	for _, item := range items {
		obj, err := convert(item)
		if err != nil {
			klog.V(4).InfoS("poll watcher skipping unconvertible item", "table", tableName, "err", err)
			continue
		}
		select {
		case w.resultCh <- watch.Event{Type: watch.Modified, Object: obj}:
		case <-ctx.Done():
			return
		}
	}
}

func (w *dynamoDBPollWatcher[T]) Stop() {
	w.cancel()
	<-w.done
}

func (w *dynamoDBPollWatcher[T]) ResultChan() <-chan watch.Event {
	return w.resultCh
}
