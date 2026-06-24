package kubeapplier

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DeleteDesire targets a single Kubernetes object on the management cluster
// for deletion.
type DeleteDesire struct {
	DynamoDBMetadata `json:"dynamodbMetadata" dynamodbav:",omitempty"`
	Spec             DeleteDesireSpec   `json:"spec"   dynamodbav:"spec"`
	Status           DeleteDesireStatus `json:"status" dynamodbav:"status"`
}

type DeleteDesireSpec struct {
	ManagementCluster string            `json:"managementCluster"      dynamodbav:"managementCluster"`
	ClusterID         string            `json:"clusterID"              dynamodbav:"clusterID"`
	NodePoolName      string            `json:"nodePoolName,omitempty" dynamodbav:"nodePoolName,omitempty"`
	TargetItem        ResourceReference `json:"targetItem,omitempty"   dynamodbav:"targetItem"`
}

type DeleteDesireStatus struct {
	Conditions               []metav1.Condition `json:"conditions,omitempty"               dynamodbav:"conditions,omitempty"`
	ObservedDesireUpdateTime time.Time          `json:"observedDesireUpdateTime,omitempty"  dynamodbav:"observedDesireUpdateTime,omitempty"`
}

func (d *DeleteDesire) GetSpecKubeContent() *runtime.RawExtension    { return nil }
func (d *DeleteDesire) SetSpecKubeContent(_ *runtime.RawExtension)   {}
func (d *DeleteDesire) GetStatusKubeContent() *runtime.RawExtension  { return nil }
func (d *DeleteDesire) SetStatusKubeContent(_ *runtime.RawExtension) {}
