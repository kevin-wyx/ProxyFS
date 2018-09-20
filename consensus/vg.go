package consensus

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// VgState represents the state of a volume group at a given point in time
type VgState int

// NOTE: When updating NodeState be sure to also update String() below.
const (
	INITIALVS   VgState = iota
	ONLININGVS          // ONLINING means VG is starting to come online on the node in the volume list
	ONLINEVS            // ONLINE means VG is online on the node in the volume list
	OFFLININGVS         // OFFLINING means the VG is gracefully going offline
	OFFLINEVS           // OFFLINE means the VG is offline and volume list is empty
	FAILEDVS            // FAILED means the VG failed on the node in the volume list
	maxVgState          // Must be last field!
)

const (
	// upDownScript is the location of the script to bring VG up or down
	upDownScript = "/vagrant/src/github.com/swiftstack/ProxyFS/consensus/proto/vg_up_down.sh"
	up           = "up"
	down         = "down"
)

func (state VgState) String() string {
	return [...]string{"INITIAL", "ONLINING", "ONLINE", "OFFLINING", "OFFLINE", "FAILED"}[state]
}

const (
	vgStr             = "VG"
	vgNameStr         = "NAME"
	vgStateStr        = "STATE"
	vgNodeStr         = "NODE"
	vgIpaddrStr       = "IPADDR"
	vgNetmaskStr      = "NETMASK"
	vgNicStr          = "NIC"
	vgEnabledStr      = "ENABLED"
	vgAutofailoverStr = "AUTOFAILOVER"
	vgVolumelistStr   = "VOLUMELIST"
)

// vgPrefix returns a string containing the vg prefix
// used for all VG keys.
func vgPrefix() string {
	return vgStr
}

// vgKeyPrefix returns a string containing the VG prefix
// with the the individual key string appended.
func vgKeyPrefix(v string) string {
	return vgPrefix() + v + ":"
}

func makeVgNameKey(n string) string {
	return vgKeyPrefix(vgNameStr) + n
}

func makeVgStateKey(n string) string {
	return vgKeyPrefix(vgStateStr) + n
}

func makeVgNodeKey(n string) string {
	return vgKeyPrefix(vgNodeStr) + n
}

func makeVgIpaddrKey(n string) string {
	return vgKeyPrefix(vgIpaddrStr) + n
}

func makeVgNetmaskKey(n string) string {
	return vgKeyPrefix(vgNetmaskStr) + n
}

func makeVgNicKey(n string) string {
	return vgKeyPrefix(vgNicStr) + n
}

func makeVgAutoFailoverKey(n string) string {
	return vgKeyPrefix(vgAutofailoverStr) + n
}

func makeVgEnabledKey(n string) string {
	return vgKeyPrefix(vgEnabledStr) + n
}

func makeVgVolumeListKey(n string) string {
	return vgKeyPrefix(vgVolumelistStr) + n
}

func (cs *Struct) getVgAttrs(vgName string, rev int64) (state string, node string,
	ipaddr string, netmask string, nic string) {

	// First grab all VG state information in one operation using the revision.
	resp, err := cs.cli.Get(context.TODO(), vgPrefix(), clientv3.WithPrefix(), clientv3.WithRev(rev))
	if err != nil {
		fmt.Printf("GET VG state failed with: %v\n", err)
		os.Exit(-1)
	}

	// Break the response out into lists.
	_, vgState, vgNode, vgIpaddr, vgNetmask, vgNic, _, _, _ := cs.parseVgResp(resp)

	// Find the attributes needed
	state = vgState[vgName]
	node = vgNode[vgName]
	ipaddr = vgIpaddr[vgName]
	netmask = vgNetmask[vgName]
	nic = vgNic[vgName]
	return
}

// onlineVg attempts to online the VG locallly.  Once
// done it will do a txn() to set the state either ONLINE
// or FAILED
// TODO - add OFFLINE of VG calling script too!
func (cs *Struct) onlineVg(vgName string, rev int64) {

	// Retrieve the VG attributes
	_, _, ipAddr, netMask, nic := cs.getVgAttrs(vgName, rev)

	// TODO - attempt online of VG and then set
	// state via txn() to either ONLINE or FAILED.
	// TODO - how long to timeout?
	fmt.Printf("onlineVg() - vgName: %v\n", vgName)

	// Run a simple command to touch a file then do txn()
	// to set VG to ONLINE
	cmd := exec.Command(upDownScript, up, vgName, ipAddr, netMask, nic)
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	fmt.Printf("CMD returned: %v\n", out.String())
	if err != nil {
		fmt.Printf("onlineVg() returned err: %v\n", err)
		cs.setVgFailed(vgName)
	}
	cs.setVgOnline(vgName)
}

// vgLocalHostEvents gets called when an event for keys with the
// prefix "VGNODE" occurs.
func (cs *Struct) vgLocalHostEvents(ev *clientv3.Event) {

	// Retrieve the VG name from the key
	vgName := strings.TrimPrefix(string(ev.Kv.Key), vgKeyPrefix(vgNodeStr))
	fmt.Printf("vgLocalHostEvents() - vg to online on local host is: %v\n", vgName)

	// Retrieve the state of the VG, if it is ONLINING then we
	// have a go routine bring the VG online.
	//
	// TODO - remove all clientv3.WithRev() since will fail if have compaction.
	// Look at redoing keys or separate query.
	vgStateKey := makeVgStateKey(vgName)
	fmt.Printf("vgLocalHostEvents() - vgStateKey %v\n", vgStateKey)
	resp, _ := cs.cli.Get(context.TODO(), vgStateKey, clientv3.WithRev(ev.Kv.ModRevision))

	// TODO - what do with error in above case - vg deleted while
	// pending events? Should not happen since cannot delete ONLINE
	// objects....
	// What other state changes must we handle?

	vgState := string(resp.Kvs[0].Value)
	fmt.Printf("vgState %v\n", vgState)
	switch vgState {
	// VG transitioned to ONLINING and local node is listed as the
	// location where the node will go ONLINE.  ONLINE the VG.
	// Go routine will do ONLINING and txn() to either set state
	// to ONLINE or FAILED when done.
	case ONLININGVS.String():
		go cs.onlineVg(vgName, ev.Kv.ModRevision)
	}
}

// vgStateWatchEvents creates a watcher based on node state
// changes.
func (cs *Struct) vgWatchEvents(swg *sync.WaitGroup) {

	// TODO - figure out what keys we watch!!!
	wch1 := cs.cli.Watch(context.Background(), vgPrefix(),
		clientv3.WithPrefix())

	swg.Done() // The watcher is running!
	for wresp1 := range wch1 {
		for _, ev := range wresp1.Events {
			fmt.Printf("vgStateWatchEvents for key: %v saw value: %v\n", string(ev.Kv.Key),
				string(ev.Kv.Value))

			// The node for a VG has changed
			if strings.HasPrefix(string(ev.Kv.Key), vgKeyPrefix(vgNodeStr)) {

				// The local node has the change
				if string(ev.Kv.Value) == cs.hostName {
					// Saw a VG event for local host.  Now extract the VG and
					cs.vgLocalHostEvents(ev)
				}
			} else if strings.HasPrefix(string(ev.Kv.Key), vgKeyPrefix(vgStateStr)) {
				vgName := strings.TrimPrefix(string(ev.Kv.Key), vgKeyPrefix(vgStateStr))
				vgState := string(ev.Kv.Value)
				fmt.Printf("vgState now: %v for vgName: %v\n", vgState, vgName)
				if vgState == INITIALVS.String() {
					// A new VG was created.  Online it.
					// TOOD - should we have a lighter weight version of
					// startVgs() that just onlines one VG?
					cs.startVgs()
				}
			}

			// TODO - what do with the VG watch events
		}

		// TODO - watcher only shutdown when local node is OFFLINE
	}
}

func (cs *Struct) parseVgResp(resp *clientv3.GetResponse) (vgName map[string]string,
	vgState map[string]string, vgNode map[string]string, vgIpaddr map[string]string,
	vgNetmask map[string]string, vgNic map[string]string, vgAutofail map[string]bool,
	vgEnabled map[string]bool, vgVolumelist map[string]string) {

	vgName = make(map[string]string)
	vgState = make(map[string]string)
	vgNode = make(map[string]string)
	vgIpaddr = make(map[string]string)
	vgNetmask = make(map[string]string)
	vgNic = make(map[string]string)
	vgAutofail = make(map[string]bool)
	vgEnabled = make(map[string]bool)
	vgVolumelist = make(map[string]string)

	for _, e := range resp.Kvs {
		if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgNameStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgNameStr))
			vgName[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgStateStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgStateStr))
			vgState[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgNodeStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgNodeStr))
			vgNode[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgIpaddrStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgIpaddrStr))
			vgIpaddr[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgNetmaskStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgNetmaskStr))
			vgNetmask[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgNicStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgNicStr))
			vgNic[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgEnabledStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgEnabledStr))
			vgEnabled[n], _ = strconv.ParseBool(string(e.Value))
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgAutofailoverStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgAutofailoverStr))
			vgAutofail[n], _ = strconv.ParseBool(string(e.Value))
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgVolumelistStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgVolumelistStr))
			vgVolumelist[n] = string(e.Value)
		}
	}

	return
}

// gatherVgInfo() gathers all VG information and node informaiton
// returns it broken out into maps.
//
// All data is taken from the same etcd global revision number.
//
// TODO - add node information too.... must be in same transacation!
// can we do WithRev(vg revision #) instead of doing a txn()?
func (cs *Struct) gatherVgInfo() (vgName map[string]string, vgState map[string]string,
	vgNode map[string]string, vgIpaddr map[string]string, vgNetmask map[string]string,
	vgNic map[string]string, vgAutofail map[string]bool, vgEnabled map[string]bool,
	vgVolumelist map[string]string, nodesAlreadyDead []string, nodesOnline []string,
	nodesHb map[string]time.Time) {

	// First grab all VG state information in one operation
	resp, err := cs.cli.Get(context.TODO(), vgPrefix(), clientv3.WithPrefix())
	if err != nil {
		fmt.Printf("GET VG state failed with: %v\n", err)
		os.Exit(-1)
	}

	// Break the response out into list of already DEAD nodes and
	// nodes which are still marked ONLINE.
	//
	// Also retrieve the last HB values for each node.
	vgName, vgState, vgNode, vgIpaddr, vgNetmask, vgNic, vgAutofail, vgEnabled,
		vgVolumelist = cs.parseVgResp(resp)
	respRev := resp.Header.GetRevision()

	// Get the node state as of the same revision number
	nodesAlreadyDead, nodesOnline, nodesHb = cs.getRevNodeState(respRev)

	return
}

// setVgsOfflineDeadNodes finds all VGs ONLINE on the newly
// DEAD node and marks the VG as OFFLINE
func (cs *Struct) setVgsOfflineDeadNodes(newlyDeadNodes []string) {

	// Retrieve VG and node state
	_, _, vgNode, _, _, _, _, _, _, _, _, _ := cs.gatherVgInfo()

	for _, deadNode := range newlyDeadNodes {
		for name, node := range vgNode {
			// If VG was ONLINE on dead node - set to OFFLINE
			if node == deadNode {
				err := cs.setVgOffline(name)
				if err != nil {
					fmt.Printf("setVgsOfflineDeadNodes() failed for vg: %v err: %v\n",
						name, err)
				}
			}

		}

	}
}

// failoverVgs is called when nodes have died.  The remaining nodes
// are scored and VGs from the failed nodes are started if possible.
// TODO - how avoid overloading a node? need weight for a VG?  what
// about priority for a VG and high priority first?
// Don't we have to use the same revision of ETCD for all these decisions?
func (cs *Struct) failoverVgs(newlyDeadNodes []string) {

	// Mark all VGs that were online on newlyDeadNodes as OFFLINE
	cs.setVgsOfflineDeadNodes(newlyDeadNodes)

	// Follow online path to online OFFLINE VGs
	cs.startVgs()
}

// parseVgOnlineInit returns a map of all VGs in either the
// ONLINE or INITIAL states
//
// This routine only adds the VG to the map if "autofailover==true" and
// "enabled=true"
func parseVgOfflineInit(vgState map[string]string, vgEnabled map[string]bool,
	vgAutofailover map[string]bool) (vgOfflineInit *list.List) {

	vgOfflineInit = list.New()
	for k, v := range vgState {
		if (vgEnabled[k] == false) || (vgAutofailover[k] == false) {
			continue
		}
		switch v {
		case INITIALVS.String():
			vgOfflineInit.PushBack(k)
		case OFFLINEVS.String():
			vgOfflineInit.PushBack(k)
		}

	}
	return
}

// setVgFailed sets the vg VGSTATE to FAILINGVS and leaves VGNODE as the
// node where it failed.
func (cs *Struct) setVgFailed(vg string) (err error) {
	var txnResp *clientv3.TxnResponse

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is still in ONLINING state
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", ONLININGVS.String()),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), FAILEDVS.String()),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	// TODO - how handle error cases????
	fmt.Printf("txnResp: %+v\n", txnResp)

	return
}

// setVgOffline sets the vg VGSTATE to OFFLINEVS and clears VGNODE.
// This is called when a node has been marked DEAD.
func (cs *Struct) setVgOffline(vg string) (err error) {
	var txnResp *clientv3.TxnResponse

	fmt.Printf("setVgOffline: name: %v\n", vg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is still in ONLINING state
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", ONLINEVS.String()),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), OFFLINEVS.String()),
		clientv3.OpPut(makeVgNodeKey(vg), ""),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	// TODO - how handle error cases????
	fmt.Printf("txnResp: %+v\n", txnResp)

	return
}

// setVgOnline sets the vg VGSTATE to ONLINEVS and leaves VGNODE as the
// node where it is online.
func (cs *Struct) setVgOnline(vg string) (err error) {
	var txnResp *clientv3.TxnResponse

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is still ONLINING
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", ONLININGVS.String()),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), ONLINEVS.String()),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	// TODO - how handle error cases????
	fmt.Printf("txnResp: %+v\n", txnResp)

	return
}

// setVgOnlining sets the vg VGSTATE to ONLININGVS and the VGNODE to the node.
//
// This transaction can fail if the node is no longer in the ONLINENS state
// or the VG is no longer in the OFFLINEVS state.
func (cs *Struct) setVgOnlining(node string, vg string) (err error) {
	var txnResp *clientv3.TxnResponse

	// Assuming that current state is INITIALVS - transition to ONLINING
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is in INITIALVS and node name is ""
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", INITIALVS.String()),
			clientv3.Compare(clientv3.Value(makeVgNodeKey(vg)), "=", ""),

		// "Then" set the values...
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), ONLININGVS.String()),
		clientv3.OpPut(makeVgNodeKey(vg), node),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	// If the VG was in INITIALVS and the txn succeeded then return now
	// TODO - review all txn() code - should I be checking this at other
	// places? Probably missing some locations...
	if txnResp.Succeeded {
		return
	}

	// Earlier transaction failed - do next transaction assuming that state
	// was OFFLINE
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is in OFFLINEVS and node name is ""
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", OFFLINEVS.String()),
			clientv3.Compare(clientv3.Value(makeVgNodeKey(vg)), "=", ""),

		// "Then" set the values...
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), ONLININGVS.String()),
		clientv3.OpPut(makeVgNodeKey(vg), node),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	if !txnResp.Succeeded {
		err = errors.New("VG no longer in INITIALVS or OFFLINEVS - possibly in ONLINING?")
		return
	}

	return
}

// offlineVgs is called when the local node has transitioned to OFFLINING.
// TODO - implement algorithm to offline the VG and then do figure out how
// to signal that local node should txn(OFFLINE)... need waitgroup from
// node side to signal that all VGs are offline???
func (cs *Struct) offlineVgs() {
	// TODO - implement this
}

// startVgs is called when a node has come ONLINE.
// TODO - implement algorithm to spread the VGs more evenly and
// in a predictable manner.
func (cs *Struct) startVgs() {

	// Retrieve VG and node state
	vgName, vgState, vgNode, vgIpaddr, vgNetmask, vgNic, vgAutofail, vgEnabled,
		vgVolumelist, nodesAlreadyDead, nodesOnline, nodesHb := cs.gatherVgInfo()

	fmt.Printf("startVgs() ---- vgName: %v vgState: %v vgNode: %v vgIpaddr: %v vgNetmask: %v\n",
		vgName, vgState, vgNode, vgIpaddr, vgNetmask)
	fmt.Printf("vgNic: %v vgAutofail: %v vgEnabled: %v vgVolumelist: %v\n",
		vgNic, vgAutofail, vgEnabled, vgVolumelist)
	fmt.Printf("nodesAlreadyDead: %v nodesOnline: %v nodesHb: %v\n",
		nodesAlreadyDead, nodesOnline, nodesHb)

	// Find VGs which are in the INITIAL or OFFLINE state
	vgsToStart := parseVgOfflineInit(vgState, vgEnabled, vgAutofail)

	cntVgsToStart := vgsToStart.Len()
	if cntVgsToStart == 0 {
		return
	}

	cntNodesOnline := len(nodesOnline)
	fmt.Printf("cntVgsToStart: %v online nodes: %v\n", cntVgsToStart, cntNodesOnline)

	// Set state to ONLINING to initiate the ONLINE
	for vgsToStart.Len() > 0 {
		for _, node := range nodesOnline {
			e := vgsToStart.Front()
			if e == nil {
				// No more VGs to online
				break
			}

			// Set the VG to online.  If the txn
			// fails then leave it on the list for
			// the next loop.
			// TODO - could this be an infinite loop
			// if all nodes are offlining?
			// TODO - this is executed in parallel on all online
			// nodes and there could fail.  We need to figure out
			// if racing nodes or failure...
			_ = cs.setVgOnlining(node, e.Value.(string))
			vgsToStart.Remove(e)
		}

	}

	// TODO - start webserver on different port - fork off script to start VIP

	// TODO - figure out which is least loaded node and start spreading the load
	// around...
	// For the initial prototype we just do round robin which does not take into
	// consideration the load of an node.  Could be case that one node is already
	// overloaded.
}

// getVgState returns the state of a VG
func (cs *Struct) getVgState(name string) (state VgState) {
	stateKey := makeVgStateKey(name)
	resp, _ := cs.cli.Get(context.TODO(), stateKey)

	stateStr := string(resp.OpResponse().Get().Kvs[0].Value)

	switch stateStr {
	case INITIALVS.String():
		return INITIALVS
	case ONLININGVS.String():
		return ONLININGVS
	case ONLINEVS.String():
		return ONLINEVS
	case OFFLININGVS.String():
		return OFFLININGVS
	case OFFLINEVS.String():
		return OFFLINEVS
	case FAILEDVS.String():
		return FAILEDVS
	}

	return
}

func (cs *Struct) checkVgExist(vgKeys []string) (err error) {
	for _, v := range vgKeys {
		resp, _ := cs.cli.Get(context.TODO(), v)
		if resp.OpResponse().Get().Count > int64(0) {
			err = errors.New("VG already exists")
			return
		}
	}
	return
}

func calcVgKeys(name string) (nameKey string, ipaddrKey string, netmaskKey string,
	nicKey string, autofailKey string, enabledKey string, stateKey string,
	nodeKey string, volumeListKey string, vgKeys []string) {

	vgKeys = make([]string, 0)
	nameKey = makeVgNameKey(name)
	vgKeys = append(vgKeys, nameKey)
	stateKey = makeVgStateKey(name)
	vgKeys = append(vgKeys, stateKey)
	nodeKey = makeVgNodeKey(name)
	vgKeys = append(vgKeys, nodeKey)
	ipaddrKey = makeVgIpaddrKey(name)
	vgKeys = append(vgKeys, ipaddrKey)
	netmaskKey = makeVgNetmaskKey(name)
	vgKeys = append(vgKeys, netmaskKey)
	nicKey = makeVgNicKey(name)
	vgKeys = append(vgKeys, nicKey)
	autofailKey = makeVgAutoFailoverKey(name)
	vgKeys = append(vgKeys, autofailKey)
	enabledKey = makeVgEnabledKey(name)
	vgKeys = append(vgKeys, enabledKey)
	volumeListKey = makeVgVolumeListKey(name)
	vgKeys = append(vgKeys, volumeListKey)

	return
}

// addVg adds a volume group
// TODO - should we add create time, failover time, etc?
func (cs *Struct) addVg(name string, ipAddr string, netMask string,
	nic string, autoFailover bool, enabled bool) (err error) {

	nameKey, ipaddrKey, netmaskKey, nicKey, autofailKey, enabledKey, stateKey,
		nodeKey, volumeListKey, vgKeys := calcVgKeys(name)

	err = cs.checkVgExist(vgKeys)
	if err != nil {
		return
	}

	// Verify that VG does not already exist which means check all
	// keys.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// Verify that the VG and it's attributes are not there.  If they are
		// the transaction will silently return.
		If(
			clientv3.Compare(clientv3.Version(nameKey), "=", 0),
			clientv3.Compare(clientv3.Version(stateKey), "=", 0),
			clientv3.Compare(clientv3.Version(nodeKey), "=", 0),
			clientv3.Compare(clientv3.Version(ipaddrKey), "=", 0),
			clientv3.Compare(clientv3.Version(netmaskKey), "=", 0),
			clientv3.Compare(clientv3.Version(nicKey), "=", 0),
			clientv3.Compare(clientv3.Version(autofailKey), "=", 0),
			clientv3.Compare(clientv3.Version(enabledKey), "=", 0),
			clientv3.Compare(clientv3.Version(volumeListKey), "=", 0),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpPut(nameKey, name),
		clientv3.OpPut(stateKey, INITIALVS.String()),
		clientv3.OpPut(nodeKey, ""),
		clientv3.OpPut(ipaddrKey, ipAddr),
		clientv3.OpPut(netmaskKey, netMask),
		clientv3.OpPut(nicKey, nic),
		clientv3.OpPut(autofailKey, strconv.FormatBool(autoFailover)),
		clientv3.OpPut(enabledKey, strconv.FormatBool(enabled)),
		clientv3.OpPut(volumeListKey, ""),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return
}

func (cs *Struct) rmVg(name string) (err error) {

	nameKey, ipaddrKey, netmaskKey, nicKey, autofailKey, enabledKey, stateKey,
		nodeKey, volumeListKey, vgKeys := calcVgKeys(name)

	err = cs.checkVgExist(vgKeys)
	if err == nil {
		err = errors.New("VG does not exist")
		return
	}

	// Don't allow a remove of a VG if ONLINING or ONLINE
	state := cs.getVgState(name)
	if (state == ONLININGVS) || (state == ONLINEVS) {
		err = errors.New("VG is in ONLINING or ONLINE state")
		return
	}

	// Verify that VG does not already exist which means check all
	// keys.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// Verify that the VG and it's attributes are not there.  If they are
		// the transaction will silently return.
		If(
			clientv3.Compare(clientv3.Version(nameKey), "!=", 0),
			clientv3.Compare(clientv3.Version(stateKey), "!=", 0),
			clientv3.Compare(clientv3.Value(stateKey), "!=", ONLINEVS.String()),
			clientv3.Compare(clientv3.Value(stateKey), "!=", ONLININGVS.String()),
			clientv3.Compare(clientv3.Version(nodeKey), "!=", 0),
			clientv3.Compare(clientv3.Version(ipaddrKey), "!=", 0),
			clientv3.Compare(clientv3.Version(netmaskKey), "!=", 0),
			clientv3.Compare(clientv3.Version(nicKey), "!=", 0),
			clientv3.Compare(clientv3.Version(autofailKey), "!=", 0),
			clientv3.Compare(clientv3.Version(enabledKey), "!=", 0),
			clientv3.Compare(clientv3.Version(volumeListKey), "!=", 0),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpDelete(nameKey),
		clientv3.OpDelete(stateKey),
		clientv3.OpDelete(nodeKey),
		clientv3.OpDelete(ipaddrKey),
		clientv3.OpDelete(netmaskKey),
		clientv3.OpDelete(nicKey),
		clientv3.OpDelete(autofailKey),
		clientv3.OpDelete(enabledKey),
		clientv3.OpDelete(volumeListKey),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return

}