package informers

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listers"
)

const defaultResyncPeriod = 30 * time.Second

type KubeApplierInformers interface {
	ApplyDesires() (cache.SharedIndexInformer, listers.ApplyDesireLister)
	ReadDesires() (cache.SharedIndexInformer, listers.ReadDesireLister)
	RunWithContext(ctx context.Context)
}

type kubeApplierInformers struct {
	applyDesireInformer cache.SharedIndexInformer
	applyDesireLister   listers.ApplyDesireLister
	readDesireInformer  cache.SharedIndexInformer
	readDesireLister    listers.ReadDesireLister
}

// NewKubeApplierInformers creates informers that watch the specs DynamoDB
// tables for desire document changes via timestamp-based polling.
// specsClient is the DynamoDB client for the specs tables.
// specsPrefix is the table name prefix (full table names are
// prefix+"-applydesires" / prefix+"-readdesires").
func NewKubeApplierInformers(
	specsClient *dynamodb.Client,
	specsPrefix string,
) KubeApplierInformers {
	return NewKubeApplierInformersWithOptions(specsClient, specsPrefix, defaultResyncPeriod, defaultPollInterval, defaultWatchDuration)
}

func NewKubeApplierInformersWithOptions(
	specsClient *dynamodb.Client,
	specsPrefix string,
	resyncPeriod time.Duration,
	pollInterval time.Duration,
	watchDuration time.Duration,
) KubeApplierInformers {
	applyTable := specsPrefix + database.TableSuffixApplyDesires
	readTable := specsPrefix + database.TableSuffixReadDesires

	applyReader := database.NewApplyDesireSpecReader(specsClient, applyTable)
	readReader := database.NewReadDesireSpecReader(specsClient, readTable)

	applyInf := newDesireInformer(
		applyTable,
		applyReader,
		func(d *kubeapplier.ApplyDesire) (runtime.Object, error) { return d, nil },
		&kubeapplier.ApplyDesire{},
		func(ctx context.Context) (runtime.Object, error) {
			items, err := applyReader.List(ctx)
			if err != nil {
				return nil, err
			}
			list := &kubeapplier.ApplyDesireList{}
			list.ResourceVersion = "0"
			for _, d := range items {
				list.Items = append(list.Items, *d)
			}
			return list, nil
		},
		resyncPeriod,
		pollInterval,
		watchDuration,
	)

	readInf := newDesireInformer(
		readTable,
		readReader,
		func(d *kubeapplier.ReadDesire) (runtime.Object, error) { return d, nil },
		&kubeapplier.ReadDesire{},
		func(ctx context.Context) (runtime.Object, error) {
			items, err := readReader.List(ctx)
			if err != nil {
				return nil, err
			}
			list := &kubeapplier.ReadDesireList{}
			list.ResourceVersion = "0"
			for _, d := range items {
				list.Items = append(list.Items, *d)
			}
			return list, nil
		},
		resyncPeriod,
		pollInterval,
		watchDuration,
	)

	return &kubeApplierInformers{
		applyDesireInformer: applyInf,
		applyDesireLister:   listers.NewApplyDesireLister(applyInf.GetIndexer()),
		readDesireInformer:  readInf,
		readDesireLister:    listers.NewReadDesireLister(readInf.GetIndexer()),
	}
}

func newDesireInformer[T any](
	tableName string,
	reader sinceReader[T],
	convert convertFn[T],
	exampleObj runtime.Object,
	listFn func(context.Context) (runtime.Object, error),
	resyncPeriod time.Duration,
	pollInterval time.Duration,
	watchDuration time.Duration,
) cache.SharedIndexInformer {
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, _ metav1.ListOptions) (runtime.Object, error) {
			return listFn(ctx)
		},
		WatchFuncWithContext: func(ctx context.Context, _ metav1.ListOptions) (watch.Interface, error) {
			return newDynamoDBPollWatcher(ctx, tableName, reader, convert, pollInterval, watchDuration), nil
		},
	}
	return cache.NewSharedIndexInformerWithOptions(
		&listWatchWithoutWatchListSemantics{lw},
		exampleObj,
		cache.SharedIndexInformerOptions{
			ResyncPeriod: resyncPeriod,
		},
	)
}

func (k *kubeApplierInformers) ApplyDesires() (cache.SharedIndexInformer, listers.ApplyDesireLister) {
	return k.applyDesireInformer, k.applyDesireLister
}

func (k *kubeApplierInformers) ReadDesires() (cache.SharedIndexInformer, listers.ReadDesireLister) {
	return k.readDesireInformer, k.readDesireLister
}

func (k *kubeApplierInformers) RunWithContext(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		k.applyDesireInformer.RunWithContext(ctx)
	}()
	go func() {
		defer wg.Done()
		k.readDesireInformer.RunWithContext(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
}
