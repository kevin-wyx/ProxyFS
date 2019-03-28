package main

import (
	"container/list"

	"github.com/swiftstack/ProxyFS/inode"
)

// References is a concept used to prevent two or more contexts from operating on
// distinct instances of a FileInode. Consider the case where each instance
// wants to lookup a previously unknown FileInode (i.e. by inode.InodeNumber).
// Each would think it is unknown and promptly create distinct instances. To
// prevent this, references to each FileInode instance will be strictly protected
// by the sync.Mutex in globalsStruct. This necessitates that a "lookup" operation
// be allowed to intrinsically create a fileInodeStruct. It also requires those
// receiving a reference to a fileInodeStruct to eventually drop their reference
// to it. Each of the globals.{unleased|sharedLease|exclusiveLease}FileInodeCacheLRU's
// and the globals.fileInodeMap must, therefore, never "forget" a fileInodeStruct
// for which a reference is still available.
//
// References occur in two cases:
//
//   A FileInode's ExtentMap is being fetched or maintained:
//
//     In this case, a single reference is made to indicate that this instance
//     is caching the FileInode's size and some or all of its ExtentMap.
//
//   A FileInode has one or more in-flight LogSegment PUTs underway:
//
//     In this case, each LogSegment PUT is represented by a reference from the
//     time it is initiated via a request to ProvisionObject() through the
//     actual LogSegment PUT until the corresponding Wrote() request has completed.
//
// Note that the File Inode Cache (globals.fileInodeMap) typically swings into action
// when initial references are made and when a last reference is released. It is,
// however, possible for movements of fileInodeStructs among the globals.*CacheLRU's
// to trigger evictions as well if, at the time, File Inode Cache limits are already
// exceeded.

func referenceFileInode(inodeNumber inode.InodeNumber) (fileInode *fileInodeStruct) {
	var (
		ok bool
	)

	globals.Lock()

	fileInode, ok = globals.fileInodeMap[inodeNumber]

	if ok {
		fileInode.references++
	} else {
		fileInode = &fileInodeStruct{
			InodeNumber:         inodeNumber,
			references:          1,
			leaseState:          fileInodeLeaseStateNone,
			sharedLockHolders:   list.New(),
			exclusiveLockHolder: nil,
			lockWaiters:         list.New(),
		}

		fileInode.cacheLRUElement = globals.unleasedFileInodeCacheLRU.PushBack(fileInode)

		globals.fileInodeMap[inodeNumber] = fileInode

		honorInodeCacheLimitsWhileLocked()
	}

	globals.Unlock()

	return
}

func (fileInode *fileInodeStruct) reference() {
	globals.Lock()

	if 0 == fileInode.references {
		logFatalf("*fileInodeStruct.reference() should not have been called with fileInode.references == 0")
	}

	fileInode.references++

	globals.Unlock()
}

func (fileInode *fileInodeStruct) dereference() {
	globals.Lock()

	fileInode.references--

	if 0 == fileInode.references {
		honorInodeCacheLimitsWhileLocked()
	}

	globals.Unlock()
}

func honorInodeCacheLimitsWhileLocked() {
	var (
		cacheLimitToEnforce      int
		fileInode                *fileInodeStruct
		fileInodeCacheLRUElement *list.Element
	)

	cacheLimitToEnforce = int(globals.config.ExclusiveFileLimit)

	for globals.exclusiveLeaseFileInodeCacheLRU.Len() > cacheLimitToEnforce {
		fileInodeCacheLRUElement = globals.exclusiveLeaseFileInodeCacheLRU.Front()
		fileInode = fileInodeCacheLRUElement.Value.(*fileInodeStruct)
		if (0 < fileInode.references) || (fileInodeLeaseStateExclusiveGranted != fileInode.leaseState) {
			break
		}
		// TODO: kick off Lease Demote
		fileInode.leaseState = fileInodeLeaseStateExclusiveDemoting
		globals.exclusiveLeaseFileInodeCacheLRU.Remove(fileInodeCacheLRUElement)
		fileInode.cacheLRUElement = globals.sharedLeaseFileInodeCacheLRU.PushBack(fileInode)
	}

	cacheLimitToEnforce = int(globals.config.SharedFileLimit)

	if globals.exclusiveLeaseFileInodeCacheLRU.Len() > int(globals.config.ExclusiveFileLimit) {
		cacheLimitToEnforce -= globals.exclusiveLeaseFileInodeCacheLRU.Len() - int(globals.config.ExclusiveFileLimit)
		if 0 > cacheLimitToEnforce {
			cacheLimitToEnforce = 0
		}
	}

	for globals.sharedLeaseFileInodeCacheLRU.Len() > cacheLimitToEnforce {
		fileInodeCacheLRUElement = globals.sharedLeaseFileInodeCacheLRU.Front()
		fileInode = fileInodeCacheLRUElement.Value.(*fileInodeStruct)
		if (0 < fileInode.references) || (fileInodeLeaseStateSharedGranted != fileInode.leaseState) {
			break
		}
		// TODO: kick off Lease Release
		fileInode.leaseState = fileInodeLeaseStateSharedReleasing
		globals.sharedLeaseFileInodeCacheLRU.Remove(fileInodeCacheLRUElement)
		fileInode.cacheLRUElement = globals.unleasedFileInodeCacheLRU.PushBack(fileInode)
	}

	cacheLimitToEnforce = int(globals.config.ExclusiveFileLimit) - globals.exclusiveLeaseFileInodeCacheLRU.Len()
	cacheLimitToEnforce += int(globals.config.SharedFileLimit) - globals.sharedLeaseFileInodeCacheLRU.Len()

	if 0 < cacheLimitToEnforce {
		cacheLimitToEnforce = 0
	}

	for globals.unleasedFileInodeCacheLRU.Len() > cacheLimitToEnforce {
		fileInodeCacheLRUElement = globals.unleasedFileInodeCacheLRU.Front()
		fileInode = fileInodeCacheLRUElement.Value.(*fileInodeStruct)
		if (0 < fileInode.references) || (fileInodeLeaseStateNone != fileInode.leaseState) {
			break
		}
		globals.unleasedFileInodeCacheLRU.Remove(fileInodeCacheLRUElement)
		delete(globals.fileInodeMap, fileInode.InodeNumber)
	}
}

// Locks and Leases are related FileInode concepts but quite different
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

// TODO: The section below needs to be totally re-worked...

func (fileInode *fileInodeStruct) requestInodeLockExclusive() (fileInodeLockRequest *fileInodeLockRequestStruct) {
	fileInodeLockRequest = &fileInodeLockRequestStruct{
		fileInode:      fileInode,
		exclusive:      true,
		holdersElement: nil,
		waitersElement: nil, // TODO: indicate always granted for now
	}

	globals.Lock()

	fileInode.exclusiveLockHolder = fileInodeLockRequest

	globals.Unlock()

	return
}

func (fileInodeLockRequest *fileInodeLockRequestStruct) release() {
	globals.Lock()

	fileInodeLockRequest.fileInode.exclusiveLockHolder = nil // TODO: for now, just release it

	globals.Unlock()
}
