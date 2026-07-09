package listers

import (
	"context"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeapplier "github.com/rrp-bot/kube-applier-aws/internal/api/kubeapplier"
	"github.com/rrp-bot/kube-applier-aws/internal/database/listertesting"
)

// fakeLister is a minimal DeleteDesireLister backed by a plain slice, used to
// stand in for the informer cache in tests without needing a real cache.Indexer.
type fakeLister struct {
	items []*kubeapplier.DeleteDesire
}

func (f *fakeLister) List() ([]*kubeapplier.DeleteDesire, error) {
	return f.items, nil
}

func (f *fakeLister) Get(documentID string) (*kubeapplier.DeleteDesire, error) {
	for _, d := range f.items {
		if d.GetDocumentID() == documentID {
			return d, nil
		}
	}
	return nil, fmt.Errorf("not found: %s", documentID)
}

func newDeleteDesire(docID string) *kubeapplier.DeleteDesire {
	d := &kubeapplier.DeleteDesire{}
	d.SetDocumentID(docID)
	d.Spec = kubeapplier.DeleteDesireSpec{
		ManagementCluster: "mc-test",
		ClusterID:         "cluster1",
		TargetItem: kubeapplier.ResourceReference{
			Version:  "v1",
			Resource: "configmaps",
			Name:     docID,
		},
	}
	return d
}

func newStatusDoc(docID string, condStatus metav1.ConditionStatus) *kubeapplier.DeleteDesire {
	d := &kubeapplier.DeleteDesire{}
	d.SetDocumentID(docID)
	d.Status = kubeapplier.DeleteDesireStatus{
		Conditions: []metav1.Condition{
			{
				Type:               kubeapplier.ConditionTypeSuccessful,
				Status:             condStatus,
				Reason:             "test",
				LastTransitionTime: metav1.Now(),
			},
		},
	}
	return d
}

// seedStatus creates a status doc in the FakeCRUD via Create.
func seedStatus(t *testing.T, crud *listertesting.FakeCRUD[kubeapplier.DeleteDesire, *kubeapplier.DeleteDesire], doc *kubeapplier.DeleteDesire) {
	t.Helper()
	if _, err := crud.Create(context.Background(), doc); err != nil {
		t.Fatalf("seed status %s: %v", doc.GetDocumentID(), err)
	}
}

func TestListPending_NoStatusDocs(t *testing.T) {
	specs := []*kubeapplier.DeleteDesire{
		newDeleteDesire("doc-a"),
		newDeleteDesire("doc-b"),
		newDeleteDesire("doc-c"),
	}
	lister := &fakeLister{items: specs}
	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.DeleteDesire, *kubeapplier.DeleteDesire]()

	pending, err := ListPendingDeleteDesires(context.Background(), lister, statusCRUD)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pending) != 3 {
		t.Errorf("expected 3 pending, got %d", len(pending))
	}
}

func TestListPending_AllSuccessful(t *testing.T) {
	specs := []*kubeapplier.DeleteDesire{
		newDeleteDesire("doc-a"),
		newDeleteDesire("doc-b"),
		newDeleteDesire("doc-c"),
	}
	lister := &fakeLister{items: specs}
	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.DeleteDesire, *kubeapplier.DeleteDesire]()
	for _, id := range []string{"doc-a", "doc-b", "doc-c"} {
		seedStatus(t, statusCRUD, newStatusDoc(id, metav1.ConditionTrue))
	}

	pending, err := ListPendingDeleteDesires(context.Background(), lister, statusCRUD)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending, got %d", len(pending))
	}
}

func TestListPending_Mixed(t *testing.T) {
	// doc-a: Successful=True  → excluded
	// doc-b: WaitingForDeletion (Successful=False) → included
	// doc-c: no status doc at all → included
	specs := []*kubeapplier.DeleteDesire{
		newDeleteDesire("doc-a"),
		newDeleteDesire("doc-b"),
		newDeleteDesire("doc-c"),
	}
	lister := &fakeLister{items: specs}
	statusCRUD := listertesting.NewFakeCRUD[kubeapplier.DeleteDesire, *kubeapplier.DeleteDesire]()
	seedStatus(t, statusCRUD, newStatusDoc("doc-a", metav1.ConditionTrue))
	seedStatus(t, statusCRUD, newStatusDoc("doc-b", metav1.ConditionFalse))

	pending, err := ListPendingDeleteDesires(context.Background(), lister, statusCRUD)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(pending))
	}
	for _, p := range pending {
		if p.GetDocumentID() == "doc-a" {
			t.Errorf("doc-a is Successful=True and should not be pending")
		}
	}
}

func TestListPending_StatusListError(t *testing.T) {
	lister := &fakeLister{items: []*kubeapplier.DeleteDesire{newDeleteDesire("doc-a")}}
	statusCRUD := &erroringCRUD{}

	pending, err := ListPendingDeleteDesires(context.Background(), lister, statusCRUD)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if pending != nil {
		t.Errorf("expected nil slice on error, got %v", pending)
	}
}

// erroringCRUD is a ResourceCRUD whose List always returns an error.
type erroringCRUD struct{}

func (e *erroringCRUD) Get(_ context.Context, _ string) (*kubeapplier.DeleteDesire, error) {
	return nil, fmt.Errorf("not implemented")
}
func (e *erroringCRUD) List(_ context.Context) ([]*kubeapplier.DeleteDesire, error) {
	return nil, fmt.Errorf("simulated DynamoDB error")
}
func (e *erroringCRUD) Create(_ context.Context, _ *kubeapplier.DeleteDesire) (*kubeapplier.DeleteDesire, error) {
	return nil, fmt.Errorf("not implemented")
}
func (e *erroringCRUD) Replace(_ context.Context, _ *kubeapplier.DeleteDesire) (*kubeapplier.DeleteDesire, error) {
	return nil, fmt.Errorf("not implemented")
}
func (e *erroringCRUD) Delete(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
