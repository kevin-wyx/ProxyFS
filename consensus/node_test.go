package consensus

import (
	"github.com/stretchr/testify/assert"
	//tu "github.com/swiftstack/ProxyFS/consensus/testutils"
	"testing"
	//	"time"
)

// Test node state diagram
func TestStateDiagram(t *testing.T) {

	testUpDown(t)
}

func testUpDown(t *testing.T) {
	assert := assert.New(t)

	cs, tc := newHA(t)
	err := cs.Server()
	assert.Nil(err, "err return for Server() should be nil")
	defer closeHA(t, cs, tc)
}

// TODO - don't start clients since want Register() to create it....
// how create with http interface, etc?
//
// Add example of how to use the test infrastructure...
//
// test:
// 1. start 3 node cluster
// 2. Register() with endpoints
// 3. Do some tests with API
// 4. Shutdown more than half of cluster
// 6. attempt transaction and get back no leader error....
