package kubeapplier

const (
	ConditionTypeSuccessful = "Successful"
	ConditionTypeDegraded   = "Degraded"
)

const (
	ConditionReasonKubeAPIError        = "KubeAPIError"
	ConditionReasonPreCheckFailed      = "PreCheckFailed"
	ConditionReasonWaitingForDeletion   = "WaitingForDeletion"
	ConditionReasonNoErrors            = "NoErrors"
	ConditionReasonFailed              = "Failed"
)
