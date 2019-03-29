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

// Locks come in two forms: Shared and Exclusive. If an Exclusive Lock has been requested
// or granted, any subsequent Shared Lock must also block lest forward progress of an
// Exclusive Lock requestor would not be guaranteed.
//
// One might imagine a desire to grab a Shared Lock and, later, determine that one actually
// needs an Exclusive Lock. Alas, this is a recipe for deadlock if two such instances both
// having obtained a Shared Lock attempting this promotion at about the same time. Neither
// would be able to promote to Exclusive because the other is stuck continuing to hold its
// Shared Lock.
//
// A better approach where such a promotion is possible is to do the reverse. Demoting an
// Exclusive Lock to a Shared Lock has no such has no deadlock concern. Hence, if it is
// possible one might ultimately need an Exclusive Lock, they should grab that first. If,
// at some point, the potential for actually needing the Lock to remain Exclusive is gone
// (but the Lock still needs to remain Shared), the Lock should then be demoted.
//
// Note, however, that it is expected Locks are actually held for very short intervals
// (e.g. in the servicing of a FUSE upcall).

// TODO
// TODO
// TODO
// TODO - need to actually implement the rest of it :-)
// TODO
// TODO
// TODO

// getSharedLock returns a granted Shared Lock if possible. If it fails, nil is returned.
func (fileInode *fileInodeStruct) getSharedLock() (grantedLock *fileInodeLockRequestStruct) {
	return nil // TODO
}

// getExclusiveLock returns a granted Exclusive Lock if possible, If it fails, nil is returned.
func (fileInode *fileInodeStruct) getExclusiveLock() (grantedLock *fileInodeLockRequestStruct) {
	return nil // TODO
}

func (grantedLock *fileInodeLockRequestStruct) release() {
	var (
		fileInode       *fileInodeStruct
		nextLock        *fileInodeLockRequestStruct
		nextLockElement *list.Element
	)

	globals.Lock()

	fileInode = grantedLock.fileInode

	_ = fileInode.sharedLockHolders.Remove(grantedLock.holdersElement)

	if grantedLock.exclusive {
		// ExclusiveLock released - see if one or more pending LockRequest's can now be granted

		nextLockElement = fileInode.lockWaiters.Front()

		if nil != nextLockElement {
			nextLock = nextLockElement.Value.(*fileInodeLockRequestStruct)

			if nextLock.exclusive {
				// Grant nextLock as ExclusiveLock

				_ = fileInode.lockWaiters.Remove(nextLock.waitersElement)
				nextLock.waitersElement = nil
				fileInode.exclusiveLockHolder = nextLock
				nextLock.Done()
			} else {
				// Grant nextLock, and any subsequent Lock's SharedLock
				//   until an ExclusiveLock Request is encountered (or no more lockWaiters)

				for {
					_ = fileInode.lockWaiters.Remove(nextLock.waitersElement)
					nextLock.waitersElement = nil
					nextLock.holdersElement = fileInode.sharedLockHolders.PushBack(nextLock)
					nextLock.Done()

					nextLockElement = fileInode.lockWaiters.Front()
					if nil == nextLockElement {
						break
					}
					nextLock = nextLockElement.Value.(*fileInodeLockRequestStruct)
					if nextLock.exclusive {
						break
					}
				}
			}
		}
	} else {
		// SharedLock released - see if one pending ExclusiveLock can now be granted

		nextLockElement = fileInode.lockWaiters.Front()

		if nil != nextLockElement {
			nextLock = nextLockElement.Value.(*fileInodeLockRequestStruct)

			// Since a subsequent SharedLock Request would have been immediately granted,
			//   we know this is an ExclusiveLock Request... so just grant it

			// TODO: That said, we can't grant it unless/until we get an ExclusiveLease

			_ = fileInode.lockWaiters.Remove(nextLock.waitersElement)
			nextLock.waitersElement = nil
			fileInode.exclusiveLockHolder = nextLock
			nextLock.Done()
		}
	}

	globals.Unlock()
}

// Leases, like Locks, also come in two forms: Shared and Exclusive. The key difference
// is that Leases are used to coordinate access among distinct pfsagentd instances. As such,
// the overhead of obtaining Leases suggests a good default behavior would be to continue
// holding a Lease even after all Locks requiring the Lease have themselves been released
// in anticipation of a new Lock request arriving shortly. Indeed, the caching of a
// FileInode's ExtentMap remains valid for the life of a Shared or Exclusive Lease and not
// having to fetch a FileInode's ExtentMap each time a read operation is performed
// provides yet another incentive to holding a Shared Lease for a much longer period of
// time.
//
// Importantly, such caching must be coordinated with other instances that may also need
// to cache. This is where Leases really shine. In order to grant a Shared Lock, this
// instance must know that no other instance holds any Exclusive Locks. To do that, a
// prerequisite for obtaining a Shared Lock is that this instance hold either a Shared
// or Exclusive Lease. Similarly, in order to grant an Exclusive Lock, this instance must
// know that no other instance holds any Shared or Exclusive Locks. To do that, a
// prerequisite for obtaining an Exclusive Lock is that this instance hold an Exclusive
// Lease.
//
// Due to write operations needing to be chained together into a smaller number of
// LogSegment PUTs, it is typical for an Exclusive Lock to be released well before
// such in-flight LogSegment PUTs have completed. And Exclusive Lease must be held,
// not only for the life span of an Exclusive Lock, but also to include the life span
// of any in-flight LogSegment PUTs.
//
// As with promotion of a Shared Lock to an Exclusive Lock being deadlock inducing, this
// concern certainly applies for the promotion of a Shared Lease to an Exclusive Lease.
// The work-around of just always requesting an Exclusive Lease "just in case" is not
// as desirebale when the duration of holding it is arbitrarily long. As such, Leases
// will, in fact, support promotion with an important caveat that it might fail. Indeed,
// it may very well occur that the Lease Manager has already issued a Revoke for a
// previously granted Shared Lease. In this case, the instance requesting the promotion
// will first have to go through the path of first releasing the Shared Lease it
// currently holds before requesting the desired Exclusive Lease.
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

// TODO
// TODO
// TODO
// TODO - need to re-work this completely...
// TODO
// TODO
// TODO

func (fileInode *fileInodeStruct) TODOrequestInodeLockExclusive() (fileInodeLockRequest *fileInodeLockRequestStruct) {
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

func (fileInodeLockRequest *fileInodeLockRequestStruct) TODOrelease() {
	globals.Lock()

	fileInodeLockRequest.fileInode.exclusiveLockHolder = nil // TODO: for now, just release it

	globals.Unlock()
}
