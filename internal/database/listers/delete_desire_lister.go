package listers

import (
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/kube-applier-aws/internal/api/kubeapplier"
)

type DeleteDesireLister interface {
	List() ([]*kubeapplier.DeleteDesire, error)
	Get(documentID string) (*kubeapplier.DeleteDesire, error)
}

type deleteDesireLister struct {
	indexer cache.Indexer
}

func NewDeleteDesireLister(indexer cache.Indexer) DeleteDesireLister {
	return &deleteDesireLister{indexer: indexer}
}

func (l *deleteDesireLister) List() ([]*kubeapplier.DeleteDesire, error) {
	return listAll[kubeapplier.DeleteDesire](l.indexer)
}

func (l *deleteDesireLister) Get(documentID string) (*kubeapplier.DeleteDesire, error) {
	return getByKey[kubeapplier.DeleteDesire](l.indexer, documentID)
}
