package listers

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/kube-applier-aws/internal/api/kubeapplier"
	"github.com/rrp-bot/kube-applier-aws/internal/database"
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

// ListPendingDeleteDesires returns all DeleteDesire specs for which deletion is
// not yet confirmed successful in the status table.
//
// Specs come from the in-memory informer cache (lister.List — zero DynamoDB
// calls). Status documents are fetched with a single Scan via statusCRUD.List,
// then indexed by documentID in memory. A spec is considered pending if:
//   - no corresponding status document exists (never reconciled), or
//   - the status document's Successful condition is absent or not True.
func ListPendingDeleteDesires(
	ctx context.Context,
	lister DeleteDesireLister,
	statusCRUD database.ResourceCRUD[kubeapplier.DeleteDesire],
) ([]*kubeapplier.DeleteDesire, error) {
	specs, err := lister.List()
	if err != nil {
		return nil, err
	}

	statuses, err := statusCRUD.List(ctx)
	if err != nil {
		return nil, err
	}

	statusByID := make(map[string]*kubeapplier.DeleteDesire, len(statuses))
	for _, s := range statuses {
		statusByID[s.GetDocumentID()] = s
	}

	var pending []*kubeapplier.DeleteDesire
	for _, spec := range specs {
		statusDoc, ok := statusByID[spec.GetDocumentID()]
		if !ok || !isSuccessfulTrue(statusDoc.Status.Conditions) {
			pending = append(pending, spec)
		}
	}
	return pending, nil
}

// isSuccessfulTrue reports whether the Successful condition is present and True.
func isSuccessfulTrue(conds []metav1.Condition) bool {
	for _, c := range conds {
		if c.Type == kubeapplier.ConditionTypeSuccessful {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}
