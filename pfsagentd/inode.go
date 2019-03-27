package main

func (fileInode *fileInodeStruct) requestInodeLock(exclusive bool, forcedReleaseChan chan struct{}) (fileInodeLockRequest *fileInodeLockRequestStruct) {
	fileInodeLockRequest = &fileInodeLockRequestStruct{
		fileInode:         fileInode,
		forcedReleaseChan: forcedReleaseChan,
		holdersElement:    nil,
		waitersElement:    nil,
	}

	// TODO: Issue necessary RPCs or block if necessary... for now just grant as Exclusive locally

	globals.Lock()

	if fileInode.lockState == fileInodeStateUnlocked {
		fileInode.lockState = fileInodeStateExclusiveLockGranted

		_ = globals.unlockedFileInodeCacheLRU.Remove(fileInode.cacheLRUElement)
		fileInode.cacheLRUElement = globals.exclusiveLockFileInodeCacheLRU.PushBack(fileInode)
	}

	fileInodeLockRequest.holdersElement = fileInode.exclusiveLockHolders.PushBack(fileInodeLockRequest)

	globals.Unlock()

	return
}

func (fileInodeLockRequest *fileInodeLockRequestStruct) release() {
	globals.Lock()

	_ = fileInodeLockRequest.fileInode.exclusiveLockHolders.Remove(fileInodeLockRequest.holdersElement)

	// TODO: For now, relinquish lock (via RPCs) if now not held locally

	if 0 == fileInodeLockRequest.fileInode.exclusiveLockHolders.Len() {
		fileInodeLockRequest.fileInode.lockState = fileInodeStateUnlocked
	}

	fileInodeLockRequest.holdersElement = nil

	globals.Unlock()
}
