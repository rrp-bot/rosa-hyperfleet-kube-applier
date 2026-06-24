package informers

import "k8s.io/client-go/tools/cache"

// listWatchWithoutWatchListSemantics wraps a cache.ListWatch to opt out of
// client-go's WatchList streaming mode. Firestore's snapshot listener does not
// emit bookmark events, which WatchList mode requires. Without this wrapper,
// the Reflector waits for a bookmark that never arrives and
// WaitForCacheSync blocks forever.
type listWatchWithoutWatchListSemantics struct {
	*cache.ListWatch
}

func (listWatchWithoutWatchListSemantics) IsWatchListSemanticsUnSupported() bool { return true }
