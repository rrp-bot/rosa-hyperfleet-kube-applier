package kubeapplier

// ResourceReference identifies a Kubernetes resource without needing a
// RESTMapper.
type ResourceReference struct {
	Group     string `json:"group"               dynamodbav:"group"`
	Version   string `json:"version"             dynamodbav:"version"`
	Resource  string `json:"resource"            dynamodbav:"resource"`
	Namespace string `json:"namespace,omitempty" dynamodbav:"namespace,omitempty"`
	Name      string `json:"name"                dynamodbav:"name"`
}
