package consensus

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	tu "github.com/swiftstack/ProxyFS/consensus/testutils"

	"github.com/swiftstack/ProxyFS/logger"
)

// Largely stolen from fs/api_test.go
func testSetup() (err error) {
	return
}

// Largely stolen from fs/api_test.go
func testTeardown() (err error) {
	return
}

// Largely stolen from fs/api_test.go
func TestMain(m *testing.M) {
	flag.Parse()

	err := testSetup()
	if nil != err {
		logger.ErrorWithError(err)
	}

	testResults := m.Run()

	err = testTeardown()
	if nil != err {
		logger.ErrorWithError(err)
	}

	os.Exit(testResults)
}

// Test basic API
func TestAPI(t *testing.T) {

	//testBasicAPI(t)
	//testAddRmVolumeGroup(t)
	testStartVolumeGroup(t)

	// To add:
	// - online, failover, verify bad input such as bad IP address?
}

// newHA sets up 3 node test cluster and opens connection to HA
func newHA(t *testing.T) (cs *EtcdConn, tc *tu.TestCluster) {

	// Start a 3 node etcd cluster
	tc = tu.NewTC(t, 3)

	// Grab endpoint used by client 0 and pass to New()
	endpoints := tc.Endpoints(0)

	assert := assert.New(t)

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cs, err := New(endpoints, tc.HostName(), 2*time.Second)
	assert.NotNil(cs, "Register() failed")
	assert.Nil(err, "Register() returned err")

	// Setup test script, etc
	cs.SetTest(true)
	cs.SetTestSWD(tc.SWD)

	return cs, tc
}

// closeHA unregisters from etcd
func closeHA(t *testing.T, cs *EtcdConn, tc *tu.TestCluster) {

	// Close our client
	cs.Close()

	// Remove cluster
	tc.Destroy(t)
}

func vgKeysToDelete(vgTestName string) (keys map[string]struct{}) {
	keys = make(map[string]struct{})
	keys[makeVgKey(vgTestName)] = struct{}{}
	return
}

func testBasicAPI(t *testing.T) {
	cs, tc := newHA(t)
	closeHA(t, cs, tc)
}

// Delete test keys
func deleteVgKeys(t *testing.T, cs *EtcdConn, km map[string]struct{}) {
	tu.DeleteKeys(t, cs.cli, km)
}

func testAddRmVolumeGroup(t *testing.T) {
	var (
		vgTestName   = "myTestVg"
		ipAddr       = "192.168.20.20"
		netMask      = "255.255.255.0"
		nic          = "eth0"
		autoFailover = true
		enabled      = true
	)
	assert := assert.New(t)

	cs, tc := newHA(t)
	defer closeHA(t, cs, tc)

	// just to be on the safe side delete the test key if it already exists
	keys := vgKeysToDelete(vgTestName)
	deleteVgKeys(t, cs, keys)

	// TODO - how add volume list to a volume group?
	// assume volumes are unique across VGs???
	err := cs.AddVolumeGroup(vgTestName, ipAddr, netMask, nic, autoFailover, enabled)
	assert.Nil(err, "AddVolumeGroup() returned err")

	// If recreate the VG again it should fail
	err = cs.AddVolumeGroup(vgTestName, ipAddr, netMask, nic, autoFailover, enabled)
	assert.NotNil(err, "AddVolumeGroup() twice should err")

	// the volume has to be offline or dead before it can be removed; let's
	// go straight to dead
	err = cs.MarkVolumeGroupFailed(vgTestName)
	assert.Nil(err, "MarkVolumeGroupFailed() returned err")

	// Now remove the volume group
	err = cs.RmVolumeGroup(vgTestName)
	assert.Nil(err, "RmVolumeGroup() returned err")

	// Trying to removing a volume group a second time should fail
	err = cs.RmVolumeGroup(vgTestName)
	assert.NotNil(err, "RmVolumeGroup() twice should err")
}

func testStartVolumeGroup(t *testing.T) {
	var (
		vgTestName      = "myTestVg"
		ipAddr          = "10.10.10.10"
		netMask         = "255.255.255.0"
		nic             = "eth0"
		autoFailover    = true
		enabled         = true
		stopWatcherChan chan struct{}    // Channel used to stop watcher
		errWatcherChan  chan error       // Channel used to return errors from watcher
		testVgChan      chan vgTestEvent // Channel use to see VG changes
	)

	// TODO - move this to startTestVgWatcher()???
	stopWatcherChan = make(chan struct{}, 1)
	errWatcherChan = make(chan error, 1)
	testVgChan = make(chan vgTestEvent, 10) // TODO - how large should it be?

	assert := assert.New(t)

	cs, tc := newHA(t)
	defer closeHA(t, cs, tc)

	keys := vgKeysToDelete(vgTestName)
	deleteVgKeys(t, cs, keys)

	// Start a watcher which will collect the state changes of
	cs.startTestVgWatcher(stopWatcherChan, errWatcherChan, testVgChan, nil)

	// Setup as a server so that startVgs() will start the VG.
	err := cs.Server()

	// TODO - block until server is ONLINE

	// Add a volume group
	err = cs.AddVolumeGroup(vgTestName, ipAddr, netMask, nic, autoFailover, enabled)
	assert.Nil(err, "AddVolumeGroup() returned err")

	err = cs.setVgOnlining(vgTestName, cs.hostName)
	assert.Nil(err, "setVgOnlining() should succeed")
	fmt.Printf("AFTER VG ONLINING\n")

	// Wait until the VG is ONLINE
	unitTestWaitVg(vgTestName, ONLINEVS, testVgChan)

	/***********************/
	time.Sleep(1 * time.Second)
	fmt.Printf("EXIT EARLY.....\n")
	return

	// Now remove the volume group - should fail since VG is in ONLINE
	// or ONLINING state.  Only VGs which are OFFLINE can be removed.
	err = cs.RmVolumeGroup(vgTestName)
	assert.NotNil(err, "RmVolumeGroup() should have returned an err")
	fmt.Printf("AFTER RM VG\n")

	// Bring the VG offline, then online, then offline, and then remove it
	err = cs.setVgOfflining(vgTestName)
	assert.Nil(err, "setVgOfflining() should succeed")
	fmt.Printf("AFTER VG OFFLINING\n")

	// Wait until the VG is OFFLINE
	unitTestWaitVg(vgTestName, OFFLINEVS, testVgChan)

	err = cs.setVgOnlining(vgTestName, cs.hostName)
	assert.Nil(err, "setVgOnlining() should succeed")
	fmt.Printf("AFTER VG ONLINING\n")

	/*
		unitTestWaitVg(vgTestName, ONLINE)
	*/

	err = cs.setVgOfflining(vgTestName)
	assert.Nil(err, "setVgOfflining() should succeed second time")

	// TODO - wait until it is OFFLINE.  Currently a race which
	// causes test to fail
	/*
		unitTestWaitVg(vgTestName, OFFLINE)
	*/

	// TODO - remove this when block waiting for
	// VG to be OFFLINE
	time.Sleep(1 * time.Second)

	// and remove the volume gorup
	err = cs.RmVolumeGroup(vgTestName)
	assert.Nil(err, "RmVolumeGroup() should now succeed")

	// TODO - wait for the VG to be removed...

	cs.stopTestWatchers(stopWatcherChan)

	// disable this node's heartbeat before exiting
	cs.Lock()
	cs.stopHB = true
	cs.Unlock()

	// Wait HB goroutine to finish
	cs.stopHBWG.Wait()

	return
}
