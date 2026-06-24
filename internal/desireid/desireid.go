package desireid

import (
	"fmt"

	"github.com/google/uuid"
)

// NamespaceUUID is the fixed UUID v5 namespace for kube-applier desire
// document IDs. Shared between the agent (this repo) and the CLM adapter
// that writes desires. Changing this value invalidates all existing document
// IDs — never change it after initial deployment.
var NamespaceUUID = uuid.MustParse("a3f1b2c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c")

// NewDocumentID computes a deterministic UUID v5 for a desire document.
// The same inputs always produce the same UUID, giving natural idempotency:
// crash-and-retry computes the same ID, so create returns conflict if it
// already exists. Different taskKey values (e.g., different field managers)
// produce different UUIDs for the same K8s resource.
func NewDocumentID(taskKey, group, version, resource, namespace, name string) string {
	input := fmt.Sprintf("%s/%s/%s/%s/%s/%s", taskKey, group, version, resource, namespace, name)
	return uuid.NewSHA1(NamespaceUUID, []byte(input)).String()
}
