package main

// Locks and Leases are related concepts but quite different
//
// Locks are short-lived used for tracking the presumably small amount of time spent in a
// read or write operation. ProxyFS itself will manage the necessary serialization at its
// end for non-R/W operations but, due to local caching, local lock management must be
// enforced.
//
// Importantly, such caching must be coordinated with other instances that may also need
// to cache. This is where Leases come into play. In order to grant a Shared Lock, this
// instance must know that no other instance holds any Exclusive Locks. To do that, a
// prerequisite for obtaining a Shared Lock is that this instance hold either a Shared
// or Exclusive Lease. Similarly, in order to grant an Exclusive Lock, this instance must
// know that no other instance holds any Shared or Exclusive Locks. To do that, a
// prerequisite for obtaining an Exclusive Lock is that this instance hold an Exclusive
// Lease.
//
// While Locks are inherently short-lived, the overhead of obtaining Leases suggests a
// good default behavior would be to continue holding a Lease even after all Locks
// requiring the Lease have been themselves been released. Indeed, the caching of a
// FileInode's ExtentMap remains valid for the life of a Shared or Exclusive Lease and
// not having to fetch a FileInode's ExtentMap each time a read operation is performed
// provides yet another incentive to holding a Shared Lease for a much longer period of
// time.
//
// In a similar vein, write operations are a particular challenge in a ProxyFS environment
// due to the need to append write data to a LogSegment rather than creating a fresh one
// for each write operation. Such long-running LogSegment PUTs are only allowed while this
// instance holds an Exclusive Lease.
//
// Note that another instance may request a Shared or Exclusive Lease that is in conflict
// with a Lease held by this instance. When this happens, a Lease Demotion (i.e. from
// Exclusive to Shared) or Lease Release will be requested by ProxyFS. At such time, any
// long-running state requiring the Lease being relinquished must itself be resolved
// (e.g. by evicting any cached ExtentMap contents and/or flushing any in-flight LogSegment
// PUTs). In addition, a loss of contact with ProxyFS (where all-instance Lease State is
// managed) must be detected by both ends (i.e. each instance and ProxyFS). If such contact
// is lost, each instance must in a timely manner force all Leases to be relinquished
// perhaps abruptly (i.e. it may not be possible to complete the flushing of any in-flight
// LogSegment PUTs). After a suitable interval, ProxyFS would then be able to reliably
// consider the instance losing contact to have relinquished all held Leases.
//
// A word about Lease Promotion must now be made. Consider two instances both holding
// a Shared Lease each determine they need to obtain an Exclusive Lease at about the
// same time. Both will make the Promotion request to ProxyFS but only one can succeed
// in this. Indeed, in order for the winning instance to obtain the promoted Lease, the
// losing instance must relinquish its Shared Lease first. It is up to ProxyFS to resolve
// this race but, nevertheless, an instance must be prepared to not only receive a failure
// to grant a Lease Promotion request, but it must also handle a request to relinquish the
// Shared Lease it currently holds. In so doing, deadlock will be avoided. Relatedly, it
// is up to ProxyFS to provide fairness such that forward progress is assured in as
// performant way as practical (e.g. by not insisting an instance immediately relinquish
// an Lease it was just granted).

func (fileInode *fileInodeStruct) requestInodeLockExclusive() (fileInodeLockRequest *fileInodeLockRequestStruct) {
	fileInodeLockRequest = &fileInodeLockRequestStruct{
		fileInode:      fileInode,
		exclusive:      true,
		holdersElement: nil,
		waitersElement: nil,
	}

	// TODO: Issue necessary RPCs or block if necessary... for now just grant as Exclusive locally

	globals.Lock()

	if fileInode.leaseState == fileInodeLeaseStateNone {
		fileInode.leaseState = fileInodeLeaseStateExclusiveGranted

		_ = globals.unleasedFileInodeCacheLRU.Remove(fileInode.cacheLRUElement)
		fileInode.cacheLRUElement = globals.exclusiveLeaseFileInodeCacheLRU.PushBack(fileInode)
	}

	fileInodeLockRequest.holdersElement = fileInode.exclusiveLockHolders.PushBack(fileInodeLockRequest)

	globals.Unlock()

	return
}

func (fileInodeLockRequest *fileInodeLockRequestStruct) release() {
	globals.Lock()

	_ = fileInodeLockRequest.fileInode.exclusiveLockHolders.Remove(fileInodeLockRequest.holdersElement)

	// TODO: For now, relinquish lease (via RPCs) if now not locked locally

	if 0 == fileInodeLockRequest.fileInode.exclusiveLockHolders.Len() {
		fileInodeLockRequest.fileInode.leaseState = fileInodeLeaseStateNone
	}

	fileInodeLockRequest.holdersElement = nil

	globals.Unlock()
}
