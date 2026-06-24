package informers

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/kube-applier-aws/internal/api/kubeapplier"
	"github.com/rrp-bot/kube-applier-aws/internal/database"
	"github.com/rrp-bot/kube-applier-aws/internal/database/listers"
)

const defaultResyncPeriod = 30 * time.Second

type KubeApplierInformers interface {
	ApplyDesires() (cache.SharedIndexInformer, listers.ApplyDesireLister)
	DeleteDesires() (cache.SharedIndexInformer, listers.DeleteDesireLister)
	ReadDesires() (cache.SharedIndexInformer, listers.ReadDesireLister)
	RunWithContext(ctx context.Context)
}

type kubeApplierInformers struct {
	applyDesireInformer  cache.SharedIndexInformer
	applyDesireLister    listers.ApplyDesireLister
	deleteDesireInformer cache.SharedIndexInformer
	deleteDesireLister   listers.DeleteDesireLister
	readDesireInformer   cache.SharedIndexInformer
	readDesireLister     listers.ReadDesireLister
}

// NewKubeApplierInformers creates informers that watch the specs DynamoDB
// tables for desire document changes. specsClient is the DynamoDB client for
// the specs tables; streamsClient is the DynamoDB Streams client used for
// change notification. specsPrefix is the table name prefix (full table names
// are prefix + "-applydesires" / "-deletedesires" / "-readdesires").
func NewKubeApplierInformers(
	specsClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	specsPrefix string,
) KubeApplierInformers {
	return NewKubeApplierInformersWithResyncPeriod(specsClient, streamsClient, specsPrefix, defaultResyncPeriod)
}

func NewKubeApplierInformersWithResyncPeriod(
	specsClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	specsPrefix string,
	resyncPeriod time.Duration,
) KubeApplierInformers {
	applyTable := specsPrefix + database.TableSuffixApplyDesires
	deleteTable := specsPrefix + database.TableSuffixDeleteDesires
	readTable := specsPrefix + database.TableSuffixReadDesires

	applyInf := newDesireInformer(
		specsClient,
		streamsClient,
		applyTable,
		&kubeapplier.ApplyDesire{},
		func(item map[string]streamtypes.AttributeValue) (runtime.Object, error) {
			// Convert stream image attributes to dynamodb/types.AttributeValue.
			return database.ItemToApplyDesire(streamImageToDynamoDBItem(item))
		},
		func(ctx context.Context) (runtime.Object, error) {
			specReader := database.NewDynamoDBKubeApplierDBClient(specsClient, specsClient, specsPrefix, specsPrefix).ApplyDesireSpecs()
			items, err := specReader.List(ctx)
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
	)

	deleteInf := newDesireInformer(
		specsClient,
		streamsClient,
		deleteTable,
		&kubeapplier.DeleteDesire{},
		func(item map[string]streamtypes.AttributeValue) (runtime.Object, error) {
			return database.ItemToDeleteDesire(streamImageToDynamoDBItem(item))
		},
		func(ctx context.Context) (runtime.Object, error) {
			specReader := database.NewDynamoDBKubeApplierDBClient(specsClient, specsClient, specsPrefix, specsPrefix).DeleteDesireSpecs()
			items, err := specReader.List(ctx)
			if err != nil {
				return nil, err
			}
			list := &kubeapplier.DeleteDesireList{}
			list.ResourceVersion = "0"
			for _, d := range items {
				list.Items = append(list.Items, *d)
			}
			return list, nil
		},
		resyncPeriod,
	)

	readInf := newDesireInformer(
		specsClient,
		streamsClient,
		readTable,
		&kubeapplier.ReadDesire{},
		func(item map[string]streamtypes.AttributeValue) (runtime.Object, error) {
			return database.ItemToReadDesire(streamImageToDynamoDBItem(item))
		},
		func(ctx context.Context) (runtime.Object, error) {
			specReader := database.NewDynamoDBKubeApplierDBClient(specsClient, specsClient, specsPrefix, specsPrefix).ReadDesireSpecs()
			items, err := specReader.List(ctx)
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
	)

	return &kubeApplierInformers{
		applyDesireInformer:  applyInf,
		applyDesireLister:    listers.NewApplyDesireLister(applyInf.GetIndexer()),
		deleteDesireInformer: deleteInf,
		deleteDesireLister:   listers.NewDeleteDesireLister(deleteInf.GetIndexer()),
		readDesireInformer:   readInf,
		readDesireLister:     listers.NewReadDesireLister(readInf.GetIndexer()),
	}
}

func newDesireInformer(
	dbClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	tableName string,
	exampleObj runtime.Object,
	streamConvertFn func(map[string]streamtypes.AttributeValue) (runtime.Object, error),
	listFn func(context.Context) (runtime.Object, error),
	resyncPeriod time.Duration,
) cache.SharedIndexInformer {
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, _ metav1.ListOptions) (runtime.Object, error) {
			return listFn(ctx)
		},
		WatchFuncWithContext: func(ctx context.Context, _ metav1.ListOptions) (watch.Interface, error) {
			return newDynamoDBStreamWatcher(ctx, dbClient, streamsClient, tableName, streamConvertFn), nil
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

func (k *kubeApplierInformers) DeleteDesires() (cache.SharedIndexInformer, listers.DeleteDesireLister) {
	return k.deleteDesireInformer, k.deleteDesireLister
}

func (k *kubeApplierInformers) ReadDesires() (cache.SharedIndexInformer, listers.ReadDesireLister) {
	return k.readDesireInformer, k.readDesireLister
}

func (k *kubeApplierInformers) RunWithContext(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		k.applyDesireInformer.RunWithContext(ctx)
	}()
	go func() {
		defer wg.Done()
		k.deleteDesireInformer.RunWithContext(ctx)
	}()
	go func() {
		defer wg.Done()
		k.readDesireInformer.RunWithContext(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
}
