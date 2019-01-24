package consensus

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/clientv3"
	mvccpb "go.etcd.io/etcd/mvcc/mvccpb"
	"sync"
)

type testConn struct {
	c chan vgTestEvent
}

// Start the goroutine(s) that watch for, and react to, node events
//
func (cs *EtcdConn) startTestVgWatcher(stopChan chan struct{}, errChan chan<- error, testVgChan chan vgTestEvent, doneWG *sync.WaitGroup) {

	// watch for changes to any key starting with the node prefix;
	wch1 := cs.cli.Watch(context.Background(), getVgKeyPrefix(),
		clientv3.WithPrefix(), clientv3.WithPrevKV())

	tc := &testConn{c: testVgChan}
	go cs.StartWatcher(wch1, tc.vgWatchTestResponse, stopChan, errChan, doneWG)
}

func (cs *EtcdConn) stopTestWatchers(stopChan chan struct{}) {
	// signal the watchers to stop
	stopChan <- struct{}{}
}

// vgWatchTestResponse is used to see changes to VGs and pass results to
// a waiting test routine.
func (tc *testConn) vgWatchTestResponse(cs *EtcdConn, response *clientv3.WatchResponse) (err error) {

	revNum := RevisionNumber(response.Header.Revision)
	for _, ev := range response.Events {

		vgInfos, vgInfoCmps, err2 := cs.unpackVgInfo(revNum, []*mvccpb.KeyValue{ev.Kv})
		if err2 != nil {
			err = err2
			fmt.Printf("vgWatchTestResponse: failed to unpack VgInfo event(s) for '%s' KV '%v'\n",
				string(ev.Kv.Key), ev.Kv)
			return
		}
		if len(vgInfos) != 1 {
			fmt.Printf("WARNING: vgWatchTestResponse: VgInfo event for '%s' has %d entries values '%v'\n",
				string(ev.Kv.Key), len(vgInfos), ev.Kv)
		}

		for vgName, newVgInfo := range vgInfos {

			// a vg changed state
			tc.vgStateChgTestEvent(revNum, vgName, newVgInfo, vgInfoCmps[vgName])
		}
	}
	return
}

type vgTestEvent struct {
	name    string
	state   VgState
	deleted bool
	node    string
}

// vgStateChgTestEvent handles any event notification for a VG state change
func (tc *testConn) vgStateChgTestEvent(revNum RevisionNumber, vgName string,
	newVgInfo *VgInfo, vgInfoCmp []clientv3.Cmp) {

	// Something about a VG has changed ... try to figure out what it was
	// and how to react

	// if the vg was deleted then there's nothing to do
	// TODO - DELETE - push to test....
	if newVgInfo.CreateRevNum == 0 {
		fmt.Printf("vgChgEvent(): vg '%s' deleted\n", vgName)
		vge := vgTestEvent{name: vgName, deleted: true}
		tc.c <- vge
		return
	}
	vge := vgTestEvent{name: vgName, state: newVgInfo.VgState}
	fmt.Printf("vgStateChgTestEvent(): SEND vge: %+v\n", vge)
	tc.c <- vge
	fmt.Printf("vgStateChgTestEvent(): END-----\n")
}

// unitTestWaitVg blocks until the VG reaches the state
func unitTestWaitVg(name string, state VgState, vgChan chan vgTestEvent) {
	fmt.Printf("unitTestWaitVg() - START\n")
	for {
		e := <-vgChan
		fmt.Printf("unitTestWaitVg() - SAW e: %v\n", e)
		if (e.name == name) && (e.state == state) {
			break
		}
	}
}
