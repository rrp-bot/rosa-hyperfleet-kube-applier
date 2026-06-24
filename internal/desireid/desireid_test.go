package desireid

import "testing"

func TestNewDocumentID_Deterministic(t *testing.T) {
	id1 := NewDocumentID("task1", "apps", "v1", "deployments", "default", "nginx")
	id2 := NewDocumentID("task1", "apps", "v1", "deployments", "default", "nginx")
	if id1 != id2 {
		t.Errorf("expected deterministic IDs, got %q and %q", id1, id2)
	}
}

func TestNewDocumentID_DifferentTaskKey(t *testing.T) {
	id1 := NewDocumentID("task1", "apps", "v1", "deployments", "default", "nginx")
	id2 := NewDocumentID("task2", "apps", "v1", "deployments", "default", "nginx")
	if id1 == id2 {
		t.Errorf("different taskKeys should produce different IDs, both got %q", id1)
	}
}

func TestNewDocumentID_DifferentResource(t *testing.T) {
	id1 := NewDocumentID("task1", "", "v1", "configmaps", "default", "my-cm")
	id2 := NewDocumentID("task1", "", "v1", "secrets", "default", "my-cm")
	if id1 == id2 {
		t.Errorf("different resources should produce different IDs, both got %q", id1)
	}
}

func TestNewDocumentID_ClusterScoped(t *testing.T) {
	id := NewDocumentID("task1", "", "v1", "namespaces", "", "kube-system")
	if id == "" {
		t.Error("expected non-empty ID for cluster-scoped resource")
	}
}

func TestNewDocumentID_ValidUUID(t *testing.T) {
	id := NewDocumentID("task1", "apps", "v1", "deployments", "default", "nginx")
	if len(id) != 36 {
		t.Errorf("expected UUID format (36 chars), got %q (%d chars)", id, len(id))
	}
}
