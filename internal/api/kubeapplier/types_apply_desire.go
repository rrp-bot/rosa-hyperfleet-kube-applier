package kubeapplier

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ApplyDesire holds a single Kubernetes object to be server-side-applied to
// the management cluster's apiserver.
type ApplyDesire struct {
	DynamoDBMetadata `json:"dynamodbMetadata" dynamodbav:",omitempty"`
	Spec             ApplyDesireSpec   `json:"spec"   dynamodbav:"spec"`
	Status           ApplyDesireStatus `json:"status" dynamodbav:"status"`
}

type ApplyDesireSpec struct {
	ManagementCluster string                `json:"managementCluster"       dynamodbav:"managementCluster"`
	ClusterID         string                `json:"clusterID"               dynamodbav:"clusterID"`
	NodePoolName      string                `json:"nodePoolName,omitempty"  dynamodbav:"nodePoolName,omitempty"`
	TargetItem        ResourceReference     `json:"targetItem"              dynamodbav:"targetItem"`
	KubeContent       *runtime.RawExtension `json:"kubeContent,omitempty"   dynamodbav:"-"`
}

type ApplyDesireStatus struct {
	Conditions                []metav1.Condition `json:"conditions,omitempty"              dynamodbav:"conditions,omitempty"`
	ObservedDesireUpdateTime  time.Time          `json:"observedDesireUpdateTime,omitempty" dynamodbav:"observedDesireUpdateTime,omitempty"`
	AppliedResourceGeneration int64              `json:"appliedResourceGeneration,omitempty" dynamodbav:"appliedResourceGeneration,omitempty"`
}

func (d *ApplyDesire) GetSpecKubeContent() *runtime.RawExtension    { return d.Spec.KubeContent }
func (d *ApplyDesire) SetSpecKubeContent(ext *runtime.RawExtension)  { d.Spec.KubeContent = ext }
func (d *ApplyDesire) GetStatusKubeContent() *runtime.RawExtension   { return nil }
func (d *ApplyDesire) SetStatusKubeContent(_ *runtime.RawExtension)  {}
