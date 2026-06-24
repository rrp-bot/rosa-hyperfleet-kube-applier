package app

import (
	"fmt"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	leaderElectionLeaseDuration = 15 * time.Second
	leaderElectionRenewDeadline = 10 * time.Second
	leaderElectionRetryPeriod   = 2 * time.Second
)

// NewLeaderElectionLock builds a Leases-backed lock in kubeNamespace named
// leaseName. leaseHolderIdentity should be the pod hostname so concurrent
// replicas distinguish themselves. The lock uses a copy of kubeconfig with
// elevated QPS/Burst so renewals are never throttled by other API traffic.
func NewLeaderElectionLock(
	leaseHolderIdentity string,
	kubeconfig *rest.Config,
	kubeNamespace string,
	leaseName string,
) (resourcelock.Interface, error) {
	leKubeconfig := rest.CopyConfig(kubeconfig)
	leKubeconfig.QPS = 20
	leKubeconfig.Burst = 40

	lock, err := resourcelock.NewFromKubeconfig(
		resourcelock.LeasesResourceLock,
		kubeNamespace,
		leaseName,
		resourcelock.ResourceLockConfig{Identity: leaseHolderIdentity},
		leKubeconfig,
		leaderElectionRenewDeadline,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create leader election lock: %w", err)
	}
	return lock, nil
}
