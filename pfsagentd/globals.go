package main

import (
	"container/list"
	"net/http"
	"os"
	"sync"
	"time"

	"bazil.org/fuse"

	"github.com/swiftstack/ProxyFS/inode"
)

type configStruct struct {
	FUSEVolumeName          string
	FUSEMountPointPath      string // Unless starting with '/', relative to $CWD
	FUSEUnMountRetryDelay   time.Duration
	FUSEUnMountRetryCap     uint64
	SwiftAuthURL            string // If domain name is used, round-robin among all will be used
	SwiftAuthUser           string
	SwiftAuthKey            string
	SwiftAccountName        string // Must be a bi-modal account
	SwiftTimeout            time.Duration
	SwiftRetryLimit         uint64
	SwiftRetryDelay         time.Duration
	SwiftRetryExpBackoff    float64
	SwiftConnectionPoolSize uint64
	ReadCacheLineSize       uint64 // Aligned chunk of a LogSegment
	ReadCacheLineCount      uint64
	SharedFileLimit         uint64
	ExclusiveFileLimit      uint64
	MaxFlushSize            uint64
	MaxFlushTime            time.Duration
	LogFilePath             string // Unless starting with '/', relative to $CWD; == "" means disabled
	LogToConsole            bool
	TraceEnabled            bool
}

type fileInodeLeaseState uint32 // Note: These are w.r.t. the state of a remote Lock Request

const (
	fileInodeLeaseStateNone fileInodeLeaseState = iota
	fileInodeLeaseStateSharedRequested
	fileInodeLeaseStateSharedGranted
	fileInodeLeaseStateSharedReleasing
	fileInodeLeaseStateSharedPromoting
	fileInodeLeaseStateExclusiveRequested
	fileInodeLeaseStateExclusiveGranted
	fileInodeLeaseStateExclusiveDemoting
	fileInodeLeaseStateExclusiveReleasing
)

type fileInodeLockRequestStruct struct {
	sync.WaitGroup
	fileInode      *fileInodeStruct
	exclusive      bool
	holdersElement *list.Element // only maintained if exclusive == false
	waitersElement *list.Element // == nil if lock granted; != nil if waiting
}

type fileInodeStruct struct {
	inode.InodeNumber
	references          uint64
	leaseState          fileInodeLeaseState
	sharedLockHolders   *list.List                  // Elements are fileInodeLockRequestStructs.holdersElement's
	exclusiveLockHolder *fileInodeLockRequestStruct // == nil if not exclusively held
	lockWaiters         *list.List                  // Front() is oldest fileInodeLockRequestStruct.waitersElement
	cacheLRUElement     *list.Element               // Element on one of globals.{unlocked|shared|exclusive}FileInodeCacheLRU
	//                                                   On globals.unleasedFileInodeCacheLRU       if leaseState one of:
	//                                                     fileInodeLeaseStateNone
	//                                                     fileInodeLeaseStateSharedReleasing
	//                                                     fileInodeLeaseStateExclusiveReleasing
	//                                                   On globals.sharedLeaseFileInodeCacheLRU    if leaseState one of:
	//                                                     fileInodeLeaseStateSharedRequested
	//                                                     fileInodeLeaseStateSharedGranted
	//                                                     fileInodeLeaseStateExclusiveDemoting
	//                                                   On globals.exclusiveLeaseFileInodeCacheLRU if leaseState one of:
	//                                                     fileInodeLeaseStateSharedPromoting
	//                                                     fileInodeLeaseStateExclusiveRequested
	//                                                     fileInodeLeaseStateExclusiveGranted
}

type globalsStruct struct {
	sync.Mutex
	config                          configStruct
	logFile                         *os.File // == nil if configStruct.LogFilePath == ""
	httpClient                      *http.Client
	retryDelay                      []time.Duration
	swiftAuthWaitGroup              *sync.WaitGroup
	swiftAuthToken                  string
	swiftAccountURL                 string // swiftStorageURL with AccountName forced to config.SwiftAccountName
	fuseConn                        *fuse.Conn
	jrpcLastID                      uint64
	fileInodeMap                    map[inode.InodeNumber]*fileInodeStruct
	unleasedFileInodeCacheLRU       *list.List // Front() is oldest fileInodeStruct.cacheLRUElement
	sharedLeaseFileInodeCacheLRU    *list.List // Front() is oldest fileInodeStruct.cacheLRUElement
	exclusiveLeaseFileInodeCacheLRU *list.List // Front() is oldest fileInodeStruct.cacheLRUElement
}

var globals globalsStruct
