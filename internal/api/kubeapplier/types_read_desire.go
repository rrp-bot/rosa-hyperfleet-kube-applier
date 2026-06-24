package kubeapplier

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ReadDesire indicates a kube item in .spec.targetItem to issue a
// list/watch+informer for, mirroring the live object into .status.kubeContent.
type ReadDesire struct {
	DynamoDBMetadata `json:"dynamodbMetadata" dynamodbav:",omitempty"`
	Spec             ReadDesireSpec   `json:"spec"   dynamodbav:"spec"`
	Status           ReadDesireStatus `json:"status" dynamodbav:"status"`
}

type ReadDesireSpec struct {
	ManagementCluster string            `json:"managementCluster"      dynamodbav:"managementCluster"`
	ClusterID         string            `json:"clusterID"              dynamodbav:"clusterID"`
	NodePoolName      string            `json:"nodePoolName,omitempty" dynamodbav:"nodePoolName,omitempty"`
	TargetItem        ResourceReference `json:"targetItem,omitempty"   dynamodbav:"targetItem"`
}

type ReadDesireStatus struct {
	Conditions               []metav1.Condition    `json:"conditions,omitempty"               dynamodbav:"conditions,omitempty"`
	ObservedDesireUpdateTime time.Time             `json:"observedDesireUpdateTime,omitempty"  dynamodbav:"observedDesireUpdateTime,omitempty"`
	KubeContent              *runtime.RawExtension `json:"kubeContent,omitempty"              dynamodbav:"-"`
}

func (d *ReadDesire) GetSpecKubeContent() *runtime.RawExtension      { return nil }
func (d *ReadDesire) SetSpecKubeContent(_ *runtime.RawExtension)     {}
func (d *ReadDesire) GetStatusKubeContent() *runtime.RawExtension    { return d.Status.KubeContent }
func (d *ReadDesire) SetStatusKubeContent(ext *runtime.RawExtension) { d.Status.KubeContent = ext }
