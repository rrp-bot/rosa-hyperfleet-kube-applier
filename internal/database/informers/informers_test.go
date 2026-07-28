package informers

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listers"
)

// --- Unit tests (no LocalStack required) ---

// fakeSinceReader is an in-memory sinceReader for unit testing the poll watcher.
type fakeSinceReader[T any] struct {
	mu    sync.Mutex
	items []*T
}

func (f *fakeSinceReader[T]) ListSince(_ context.Context, since time.Time) ([]*T, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*T(nil), f.items...), nil
}

func (f *fakeSinceReader[T]) setItems(items []*T) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items = items
}

func TestPollWatcher_DeliversModifiedEvents(t *testing.T) {
	reader := &fakeSinceReader[kubeapplier.ApplyDesire]{}

	d1 := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--a"},
		Spec:             kubeapplier.ApplyDesireSpec{ClusterID: "c1"},
	}
	d2 := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--b"},
		Spec:             kubeapplier.ApplyDesireSpec{ClusterID: "c1"},
	}
	reader.setItems([]*kubeapplier.ApplyDesire{d1, d2})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := newDynamoDBPollWatcher(
		ctx,
		"test-table",
		reader,
		func(d *kubeapplier.ApplyDesire) (runtime.Object, error) { return d, nil },
		10*time.Millisecond,  // fast poll for test
		10*time.Minute,       // long watch duration so it doesn't close during test
	)

	received := map[string]bool{}
	timeout := time.After(2 * time.Second)
	for len(received) < 2 {
		select {
		case event, ok := <-w.ResultChan():
			if !ok {
				t.Fatal("result channel closed unexpectedly")
			}
			if event.Type != watch.Modified {
				t.Errorf("expected Modified event, got %v", event.Type)
			}
			d, ok := event.Object.(*kubeapplier.ApplyDesire)
			if !ok {
				t.Fatalf("unexpected object type %T", event.Object)
			}
			received[d.DocumentID] = true
		case <-timeout:
			t.Fatalf("timed out waiting for events; received %v", received)
		}
	}

	if !received["c1--a"] || !received["c1--b"] {
		t.Errorf("did not receive expected document IDs; got %v", received)
	}
	w.Stop()
}

func TestPollWatcher_ClosesAfterWatchDuration(t *testing.T) {
	reader := &fakeSinceReader[kubeapplier.ApplyDesire]{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchDuration := 50 * time.Millisecond
	w := newDynamoDBPollWatcher(
		ctx,
		"test-table",
		reader,
		func(d *kubeapplier.ApplyDesire) (runtime.Object, error) { return d, nil },
		1*time.Second,  // poll interval longer than watch duration — no polls expected
		watchDuration,
	)

	// The result channel should be closed after watchDuration.
	select {
	case _, ok := <-w.ResultChan():
		if ok {
			t.Error("expected channel to be closed, got an event")
		}
		// Channel closed as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("result channel was not closed after watch duration elapsed")
	}
}

func TestPollWatcher_StopClosesChannel(t *testing.T) {
	reader := &fakeSinceReader[kubeapplier.ApplyDesire]{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := newDynamoDBPollWatcher(
		ctx,
		"test-table",
		reader,
		func(d *kubeapplier.ApplyDesire) (runtime.Object, error) { return d, nil },
		1*time.Hour,
		1*time.Hour,
	)

	w.Stop()

	select {
	case _, ok := <-w.ResultChan():
		if ok {
			t.Error("expected channel to be closed after Stop()")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("result channel was not closed after Stop()")
	}
}

func TestListWatchWithoutWatchListSemantics(t *testing.T) {
	lw := listWatchWithoutWatchListSemantics{&cache.ListWatch{}}
	if !lw.IsWatchListSemanticsUnSupported() {
		t.Error("expected IsWatchListSemanticsUnSupported to return true")
	}
}

func TestListerListFromPopulatedCache(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})

	desires := []*kubeapplier.ApplyDesire{
		{DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--a"}},
		{DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--b"}},
		{DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c2--a"}},
	}
	for _, d := range desires {
		if err := indexer.Add(d); err != nil {
			t.Fatalf("indexer.Add: %v", err)
		}
	}

	lister := listers.NewApplyDesireLister(indexer)

	items, err := lister.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("List returned %d items, want 3", len(items))
	}
}

func TestListerGetFromPopulatedCache(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--a"},
		Spec:             kubeapplier.ApplyDesireSpec{ClusterID: "c1"},
	}
	if err := indexer.Add(d); err != nil {
		t.Fatalf("indexer.Add: %v", err)
	}

	lister := listers.NewApplyDesireLister(indexer)

	got, err := lister.Get("c1--a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "c1")
	}
}

func TestListerGetNotFound(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	lister := listers.NewApplyDesireLister(indexer)

	_, err := lister.Get("nonexistent")
	if !database.IsNotFoundError(err) {
		t.Errorf("expected NotFoundError, got %v", err)
	}
}

func TestReadDesireListerFromPopulatedCache(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	d := &kubeapplier.ReadDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--read1"},
		Spec:             kubeapplier.ReadDesireSpec{ClusterID: "c1"},
	}
	if err := indexer.Add(d); err != nil {
		t.Fatalf("indexer.Add: %v", err)
	}

	lister := listers.NewReadDesireLister(indexer)

	got, err := lister.Get("c1--read1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "c1")
	}

	items, err := lister.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 1 {
		t.Errorf("List returned %d items, want 1", len(items))
	}
}

// --- Integration tests (require LOCALSTACK_ENDPOINT) ---

func requireLocalStack(t *testing.T) {
	t.Helper()
	if os.Getenv("LOCALSTACK_ENDPOINT") == "" {
		t.Skip("LOCALSTACK_ENDPOINT not set; skipping integration test")
	}
}

func newLocalStackClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithBaseEndpoint(endpoint),
	)
	if err != nil {
		t.Fatalf("awsconfig.LoadDefaultConfig: %v", err)
	}
	return dynamodb.NewFromConfig(cfg)
}

func createTestTable(t *testing.T, dbClient *dynamodb.Client, tableName string) {
	t.Helper()
	ctx := context.Background()
	_, err := dbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: dbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []dbtypes.AttributeDefinition{
			{AttributeName: aws.String("documentID"), AttributeType: dbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []dbtypes.KeySchemaElement{
			{AttributeName: aws.String("documentID"), KeyType: dbtypes.KeyTypeHash},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable %s: %v", tableName, err)
	}
	t.Cleanup(func() {
		dbClient.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})
}

func startAndSync(t *testing.T, ctx context.Context, info KubeApplierInformers) {
	t.Helper()
	go info.RunWithContext(ctx)
	applyInf, _ := info.ApplyDesires()
	readInf, _ := info.ReadDesires()
	if !cache.WaitForCacheSync(ctx.Done(), applyInf.HasSynced, readInf.HasSynced) {
		t.Fatal("informers did not sync")
	}
}

func waitForCacheCount(t *testing.T, store cache.Store, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if len(store.List()) == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for cache to contain %d items (has %d)", want, len(store.List()))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// newTestInformers creates informers with short poll/watch durations suitable
// for integration tests.
func newTestInformers(dbClient *dynamodb.Client, prefix string) KubeApplierInformers {
	return NewKubeApplierInformersWithOptions(
		dbClient,
		prefix,
		30*time.Second,  // resync period
		500*time.Millisecond, // fast poll for tests
		10*time.Second,  // watch duration — triggers a re-list every 10s in tests
	)
}

func TestIntegration_InformerSyncsExistingDocuments(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient := newLocalStackClient(t)
	prefix := fmt.Sprintf("inf-existing-%d", time.Now().UnixNano())

	applyTable := prefix + database.TableSuffixApplyDesires
	readTable := prefix + database.TableSuffixReadDesires
	createTestTable(t, dbClient, applyTable)
	createTestTable(t, dbClient, readTable)

	dbCRUD := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefix, prefix)

	for i := 0; i < 3; i++ {
		d := &kubeapplier.ApplyDesire{
			DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: fmt.Sprintf("c1--item%d", i)},
			Spec: kubeapplier.ApplyDesireSpec{
				ManagementCluster: "mc-test",
				ClusterID:         "c1",
				TargetItem: kubeapplier.ResourceReference{
					Version:  "v1",
					Resource: "configmaps",
					Name:     fmt.Sprintf("cm-%d", i),
				},
			},
		}
		if _, err := dbCRUD.ApplyDesireStatus().Create(ctx, d); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	info := newTestInformers(dbClient, prefix)
	startAndSync(t, ctx, info)

	applyInf, applyLister := info.ApplyDesires()
	if len(applyInf.GetStore().List()) != 3 {
		t.Errorf("expected 3 items in cache, got %d", len(applyInf.GetStore().List()))
	}

	items, err := applyLister.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("lister returned %d items, want 3", len(items))
	}

	got, err := applyLister.Get("c1--item1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "c1")
	}
}

func TestIntegration_PollDeliversEvents(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient := newLocalStackClient(t)
	prefix := fmt.Sprintf("inf-poll-%d", time.Now().UnixNano())

	applyTable := prefix + database.TableSuffixApplyDesires
	readTable := prefix + database.TableSuffixReadDesires
	createTestTable(t, dbClient, applyTable)
	createTestTable(t, dbClient, readTable)

	dbCRUD := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefix, prefix)
	crud := dbCRUD.ApplyDesireStatus()

	info := newTestInformers(dbClient, prefix)
	startAndSync(t, ctx, info)

	applyInf, _ := info.ApplyDesires()

	// Create a document — the poll watcher should deliver it within poll interval.
	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--live"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "c1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "live-cm",
			},
		},
	}
	created, err := crud.Create(ctx, d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Item should appear in cache within a few poll intervals.
	waitForCacheCount(t, applyInf.GetStore(), 1, 15*time.Second)

	// Modify the document.
	created.Spec.ClusterID = "c2"
	if _, err := crud.Replace(ctx, created); err != nil {
		t.Fatalf("Replace: %v", err)
	}

	// Wait for the modification to propagate into the cache.
	deadline := time.After(15 * time.Second)
	for {
		item, exists, _ := applyInf.GetStore().GetByKey("c1--live")
		if exists {
			if item.(*kubeapplier.ApplyDesire).Spec.ClusterID == "c2" {
				break
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for modification in cache")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestIntegration_PerTableIsolation(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient := newLocalStackClient(t)
	prefixA := fmt.Sprintf("inf-iso-a-%d", time.Now().UnixNano())
	prefixB := fmt.Sprintf("inf-iso-b-%d", time.Now().UnixNano())

	for _, prefix := range []string{prefixA, prefixB} {
		createTestTable(t, dbClient, prefix+database.TableSuffixApplyDesires)
		createTestTable(t, dbClient, prefix+database.TableSuffixReadDesires)
	}

	dbCRUDA := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefixA, prefixA)

	infoA := newTestInformers(dbClient, prefixA)
	infoB := newTestInformers(dbClient, prefixB)
	startAndSync(t, ctx, infoA)
	startAndSync(t, ctx, infoB)

	applyInfA, _ := infoA.ApplyDesires()
	applyInfB, _ := infoB.ApplyDesires()

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--isolated"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-a",
			ClusterID:         "c1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "iso-cm",
			},
		},
	}
	if _, err := dbCRUDA.ApplyDesireStatus().Create(ctx, d); err != nil {
		t.Fatalf("Create in A: %v", err)
	}

	waitForCacheCount(t, applyInfA.GetStore(), 1, 15*time.Second)

	// B should remain empty.
	time.Sleep(500 * time.Millisecond)
	if len(applyInfB.GetStore().List()) != 0 {
		t.Errorf("expected 0 items in B's cache, got %d", len(applyInfB.GetStore().List()))
	}
}
