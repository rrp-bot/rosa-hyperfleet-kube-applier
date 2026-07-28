package database

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dyndbu "github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-db/dynamodb"
)

// rawExtField names — delegated to the central hyperfleet-db/dynamodb lib.
const (
	rawExtFieldSpecKubeContent   = dyndbu.RawExtFieldSpecKubeContent
	rawExtFieldStatusKubeContent = dyndbu.RawExtFieldStatusKubeContent
)

// KubeContentAccessor — type alias to the central lib's interface so callers
// in this package (crud.go) continue to compile without changes.
type KubeContentAccessor = dyndbu.KubeContentAccessor

// kubeContentAttributeValues delegates to the central lib.
func kubeContentAttributeValues(acc KubeContentAccessor) (map[string]types.AttributeValue, error) {
	return dyndbu.KubeContentAttributeValues(acc)
}

// kubeContentReadFromItem delegates to the central lib.
func kubeContentReadFromItem(acc KubeContentAccessor, item map[string]types.AttributeValue) error {
	return dyndbu.KubeContentReadFromItem(acc, item)
}
