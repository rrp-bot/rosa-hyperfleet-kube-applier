package kubeapplier

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// -- ApplyDesire runtime.Object + ObjectMetaAccessor --

var (
	_ runtime.Object            = &ApplyDesire{}
	_ metav1.ObjectMetaAccessor = &ApplyDesire{}
)

func (o *ApplyDesire) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (o *ApplyDesire) GetObjectMeta() metav1.Object {
	return &metav1.ObjectMeta{Name: o.DocumentID}
}

// ApplyDesireList is a list of ApplyDesire resources compatible with
// runtime.Object for use with Kubernetes informer machinery.
type ApplyDesireList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ApplyDesire `json:"items"`
}

var _ runtime.Object = &ApplyDesireList{}

func (l *ApplyDesireList) GetObjectKind() schema.ObjectKind { return &l.TypeMeta }

// -- DeleteDesire runtime.Object + ObjectMetaAccessor --

var (
	_ runtime.Object            = &DeleteDesire{}
	_ metav1.ObjectMetaAccessor = &DeleteDesire{}
)

func (o *DeleteDesire) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (o *DeleteDesire) GetObjectMeta() metav1.Object {
	return &metav1.ObjectMeta{Name: o.DocumentID}
}

// DeleteDesireList is a list of DeleteDesire resources compatible with
// runtime.Object for use with Kubernetes informer machinery.
type DeleteDesireList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DeleteDesire `json:"items"`
}

var _ runtime.Object = &DeleteDesireList{}

func (l *DeleteDesireList) GetObjectKind() schema.ObjectKind { return &l.TypeMeta }

// -- ReadDesire runtime.Object + ObjectMetaAccessor --

var (
	_ runtime.Object            = &ReadDesire{}
	_ metav1.ObjectMetaAccessor = &ReadDesire{}
)

func (o *ReadDesire) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (o *ReadDesire) GetObjectMeta() metav1.Object {
	return &metav1.ObjectMeta{Name: o.DocumentID}
}

// ReadDesireList is a list of ReadDesire resources compatible with
// runtime.Object for use with Kubernetes informer machinery.
type ReadDesireList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ReadDesire `json:"items"`
}

var _ runtime.Object = &ReadDesireList{}

func (l *ReadDesireList) GetObjectKind() schema.ObjectKind { return &l.TypeMeta }
