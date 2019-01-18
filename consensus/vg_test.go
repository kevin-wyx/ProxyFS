package consensus

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/clientv3"
	//mvccpb "go.etcd.io/etcd/mvcc/mvccpb"
	"sync"
)

// Start the goroutine(s) that watch for, and react to, node events
//
func (cs *EtcdConn) startTestVgWatcher(stopChan chan struct{}, errChan chan<- error, doneWG *sync.WaitGroup) {

	// watch for changes to any key starting with the node prefix;
	wch1 := cs.cli.Watch(context.Background(), getVgKeyPrefix(),
		clientv3.WithPrefix(), clientv3.WithPrevKV())

	fmt.Printf("startTestVgWatcher() stopChan: %v\n", stopChan)

	go cs.StartWatcher(wch1, vgWatchTestResponse, stopChan, errChan, doneWG)
}

func (cs *EtcdConn) stopTestWatchers(stopChan chan struct{}) {

	fmt.Printf("Sending STOP - stopChan: %v\n", stopChan)
	// signal the watchers to stop
	stopChan <- struct{}{}
}

func vgWatchTestResponse(cs *EtcdConn, response *clientv3.WatchResponse) (err error) {

	/*
		revNum := RevisionNumber(response.Header.Revision)
		for _, ev := range response.Events {

				vgInfos, vgInfoCmps, err2 := cs.unpackVgInfo(revNum, []*mvccpb.KeyValue{ev.Kv})
				if err2 != nil {
					err = err2
					fmt.Printf("vgWatchResponse: failed to unpack VgInfo event(s) for '%s' KV '%v'\n",
						string(ev.Kv.Key), ev.Kv)
					return
				}
				if len(vgInfos) != 1 {
					fmt.Printf("WARNING: vgWatchResponse: VgInfo event for '%s' has %d entries values '%v'\n",
						string(ev.Kv.Key), len(vgInfos), ev.Kv)
				}

				for vgName, newVgInfo := range vgInfos {

					cs.Lock()
					vgInfo, ok := cs.vgMap[vgName]
					if !ok || newVgInfo.VgState != vgInfo.VgState {

						// a vg changed state
						cs.vgStateChgTestEvent(revNum, vgName, newVgInfo, vgInfoCmps[vgName])
					}

					// if the vg still exists update the info and revision
					// numbers (whether they changed or not)
					vgInfo, ok = cs.vgMap[vgName]
					if ok {
						vgInfo.VgInfoValue = newVgInfo.VgInfoValue
						vgInfo.EtcdKeyHeader = newVgInfo.EtcdKeyHeader
					}
					cs.Unlock()
				}
		}
	*/
	return
}

// vgChgEvent handles any event notification for a VG state change (the
// KeyValue, ev.Kv, must be for a Volume Group).
//
// cs.Lock() is held on entry and should be held until exit.
//
func (cs *EtcdConn) vgStateChgTestEvent(revNum RevisionNumber, vgName string,
	newVgInfo *VgInfo, vgInfoCmp []clientv3.Cmp) {

	// Something about a VG has changed ... try to figure out what it was
	// and how to react

	// if the vg was deleted then there's nothing to do
	if newVgInfo.CreateRevNum == 0 {
		fmt.Printf("vgChgEvent(): vg '%s' deleted\n", vgName)
		delete(cs.vgMap, vgName)
		return
	}

	fmt.Printf("vgStateChgTestEvent(): vg '%s' newVgInfo %v  node '%s' state '%v'\n",
		vgName, newVgInfo, newVgInfo.VgNode, newVgInfo.VgState)

	var (
		conditionals = make([]clientv3.Cmp, 0, 1)
		//operations   = make([]clientv3.Op, 0, 1)
	)
	conditionals = append(conditionals, vgInfoCmp...)

	/*
		switch newVgInfo.VgState {

		case INITIALVS:
			if !cs.server {
				return
			}

			// This node is probably not in the map, so add it
			cs.Lock()
			cs.vgMap[vgName] = newVgInfo
			cs.Unlock()

			// A new VG was created.  If this node is a server then set the
			// VG to onlining on this node (if multiple nodes are up this is
			// a race to see which node wins).
			//
			// TODO: make a placement decision
			newVgInfo.VgState = ONLININGVS
			newVgInfo.VgNode = cs.hostName
			putOps, err := cs.putVgInfo(vgName, newVgInfo)
			if err != nil {
				fmt.Printf("Hmmm. putVgInfo(%s, %v) failed: %s\n",
					vgName, newVgInfo, err)
				return
			}

			operations = append(operations, putOps...)

		case ONLININGVS:
			// If VG onlining on local host then start the online
			if newVgInfo.VgNode != cs.hostName {
				return
			}
			if !cs.server {
				fmt.Printf("ERROR: VG '%s' is onlining on node %s but %s is not a server\n",
					vgName, newVgInfo.VgNode, cs.hostName)
				return
			}

			// Assume there is only one thread on this node that handles
			// events for this volume so there is only one caller to
			// callUpDownScript() at a time.
			//
			// However, this will be run each time there is a state change
			// for this VG (i.e. if any field changes).
			fmt.Printf("vgChgEvent() - vgName: %s - LOCAL - ONLINING\n", vgName)

			err := cs.callUpDownScript(upOperation, vgName,
				newVgInfo.VgIpAddr, newVgInfo.VgNetmask, newVgInfo.VgNic)
			if err != nil {
				fmt.Printf("WARNING: UpDownScript UP for VG %s IPaddr %s netmask %s nic %s failed: %s\n",
					vgName, newVgInfo.VgIpAddr, newVgInfo.VgNetmask, newVgInfo.VgNic, err)

				// old code would set to failed at this point
				// newVgInfo.VgState = FAILEDVS
				// putOp := cs.putVgInfo(vgName, *newVgInfo)
				return
			}
			newVgInfo.VgState = ONLINEVS
			putOps, err := cs.putVgInfo(vgName, newVgInfo)
			if err != nil {
				fmt.Printf("Hmmm. putVgInfo(%s, %v) failed: %s\n",
					vgName, newVgInfo, err)
				return
			}
			operations = append(operations, putOps...)

		case ONLINEVS:
			// the VG is now online, so there's no work to do ...
			return

		case OFFLININGVS:
			// A VG is offlining.  If its on this node and we're the server, do something ...
			if newVgInfo.VgNode != cs.hostName {
				return
			}
			if !cs.server {
				fmt.Printf("ERROR: VG '%s' is offlining on node %s but %s is not a server\n",
					vgName, newVgInfo.VgNode, cs.hostName)
				return
			}
			fmt.Printf("vgChgEvent() - vgName: %s - LOCAL - OFFLINING\n", vgName)

			err := cs.callUpDownScript(downOperation, vgName,
				newVgInfo.VgIpAddr, newVgInfo.VgNetmask, newVgInfo.VgNic)
			if err != nil {
				fmt.Printf("WARNING: UpDownScript Down for VG %s IPaddr %s netmask %s nic %s failed: %s\n",
					vgName, newVgInfo.VgIpAddr, newVgInfo.VgNetmask, newVgInfo.VgNic, err)
				// old code would set to failed at this point
				// newVgInfo.VgState = FAILEDVS
				// putOp := cs.putVgInfo(vgName, *newVgInfo)
				return
			}

			newVgInfo.VgState = OFFLINEVS
			newVgInfo.VgNode = ""
			putOps, err := cs.putVgInfo(vgName, newVgInfo)
			if err != nil {
				fmt.Printf("Hmmm. putVgInfo(%s, %v) failed: %s\n",
					vgName, newVgInfo, err)
				return
			}

			operations = append(operations, putOps...)

		case OFFLINEVS:
			// We have finished offlining this VG.
			//
			// If we are in the CLI then signal that we are done offlining this VG.
			if !cs.server {
				// Wakeup blocked CLI if waiting for this VG
				if cs.offlineVg && vgName == cs.vgName {
					cs.cliWG.Done()
				}
				return
			}

			// If the local node is in OFFLINING and all VGs are OFFLINE
			// then transition to DEAD, otherwise there's nothing to do.
			nodeInfo, nodeInfoCmp, err := cs.getNodeInfo(cs.hostName, revNum)
			if err != nil {
				// not sure what we can other than  return
				fmt.Printf("vgChgEvent(): cs.getNodeInfo() revNum %d host '%s' failed: %s\n",
					revNum, cs.hostName, err)
				return
			}
			if nodeInfo == nil {
				// it seems like this can't happen
				err = fmt.Errorf("vgChgEvent(): cs.getNodeInfo() revNum %d host '%s' does not exist",
					revNum, cs.hostName)
				fmt.Printf("WARNING: %s\n", err)
				return
			}

			if nodeInfo.NodeState != OFFLINING {
				// nothing else to do
				return
			}
			conditionals = append(conditionals, nodeInfoCmp...)

			allDown, compares := cs.allVgsDown(revNum)
			if !allDown {
				return
			}
			conditionals = append(conditionals, compares...)

			// All VGs are down - now transition the node to DEAD.
			// TODO: this should be a transaction OP that checks conditionals
			cs.updateNodeState(cs.hostName, revNum, DEAD, nil)
			return

		case FAILEDVS:
			// the volume group is now failed; there's nothing else to do
			return

		default:
			fmt.Printf("vgChgEvent(): vg '%s' has unknown state '%v'\n",
				vgName, newVgInfo.VgState)
		}

		// update the shared state (or fail)
		txnResp, err := cs.updateEtcd(conditionals, operations)
	*/

	// TODO: should we retry on failure?
	//fmt.Printf("vgStateChgTestEvent(): txnResp: %v err %v\n", txnResp, err)
	fmt.Printf("vgStateChgTestEvent(): END-----\n")
}
