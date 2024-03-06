package paxos

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"6.5840/labrpc"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type config struct {
	mu        sync.Mutex
	t         *testing.T
	finished  int32
	net       *labrpc.Network
	n         int
	pxa       []*Paxos
	connected []bool // whether each server is on the net
	saved     []*Persister
	endnames  [][]string // the port file names each sends to
	start     time.Time  // time at which make_config() was called
	// begin()/end() statistics
	t0     time.Time // time at which test_test.go called cfg.begin()
	rpcs0  int       // rpcTotal() at start of test
	bytes0 int64
}

var ncpu_once sync.Once

func make_config(t *testing.T, n int, unreliable bool) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.pxa = make([]*Paxos, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.start = time.Now()

	cfg.setunreliable(unreliable)

	fmt.Print("\nSetting: LongDelays: ")
	if rand.Intn(2) == 0 {
		cfg.net.LongDelays(true)
		fmt.Println("true")
	} else {
		cfg.net.LongDelays(false)
		fmt.Println("false")
	}

	for i := 0; i < cfg.n; i++ {
		cfg.start1(i)
	}

	// connect everyone
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	return cfg
}

func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // disable client connections to the server.

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	px := cfg.pxa[i]
	if px != nil {
		cfg.mu.Unlock()
		px.Kill()
		cfg.mu.Lock()
		cfg.pxa[i] = nil
	}

	if cfg.saved[i] != nil {
		state, pinsts := cfg.saved[i].ReadPaxosState()
		cfg.saved[i] = &Persister{}
		cfg.saved[i].Save(state, pinsts)
	}
}

func (cfg *config) start1(i int) {
	cfg.crash1(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	cfg.mu.Lock()

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	px := Make(ends, i, cfg.saved[i])

	cfg.mu.Lock()
	cfg.pxa[i] = px
	cfg.mu.Unlock()

	svc := labrpc.MakeService(px)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) checkTimeout() {
	// enforce a ten minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 600*time.Second {
		cfg.t.Fatal("test took longer than 600 seconds")
	}
}

func (cfg *config) checkFinished() bool {
	z := atomic.LoadInt32(&cfg.finished)
	return z != 0
}

func (cfg *config) cleanup(wait time.Duration) {
	atomic.StoreInt32(&cfg.finished, 1)
	for i := 0; i < len(cfg.pxa); i++ {
		if cfg.pxa[i] != nil {
			cfg.pxa[i].Kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
	time.Sleep(time.Second * wait)
	if runtime.NumGoroutine() >= 10 {
		cfg.t.Fatal("Still too many goroutines after cleanup")
	}
}

// attach server i to the net.
func (cfg *config) connect(i int) {
	// fmt.Printf("connect(%d)\n", i)

	cfg.connected[i] = true

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, true)
		}
	}
}

// detach server i from the net.
func (cfg *config) disconnect(i int) {
	// fmt.Printf("disconnect(%d)\n", i)

	cfg.connected[i] = false

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, false)
		}
	}
}
func (cfg *config) disconnectIncome(i int) {
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) bytesTotal() int64 {
	return cfg.net.GetTotalBytes()
}

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}

// start a Test.
// print the Test message.
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	cfg.bytes0 = cfg.bytesTotal()
}

// end a Test -- the fact that we got here means there
// was no failure.
// print the Passed message,
// and some performance numbers.
func (cfg *config) end() {
	cfg.checkTimeout()
	if !cfg.t.Failed() {
		cfg.mu.Lock()
		t := time.Since(cfg.t0).Seconds()       // real time
		npeers := cfg.n                         // number of Raft peers
		nrpc := cfg.rpcTotal() - cfg.rpcs0      // number of RPC sends
		nbytes := cfg.bytesTotal() - cfg.bytes0 // number of bytes
		cfg.mu.Unlock()

		fmt.Printf("  ... Passed --")
		fmt.Printf("  %4.1f  %d %4d %7d\n", t, npeers, nrpc, nbytes)
	}
}

func (cfg *config) ndecided(seq int) int {
	count := 0
	var v interface{}
	for i := 0; i < len(cfg.pxa); i++ {
		if cfg.pxa[i] != nil {
			decided, v1 := cfg.pxa[i].Status(seq)
			if decided == Decided {
				if count > 0 && v != v1 {
					cfg.t.Fatalf("decided values do not match; seq=%v i=%v v=%v v1=%v",
						seq, i, v, v1)
				}
				count++
				v = v1
			}
		}
	}
	return count
}

func (cfg *config) waitn(seq int, wanted int) {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if cfg.ndecided(seq) >= wanted {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
	nd := cfg.ndecided(seq)
	if nd < wanted {
		cfg.t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
	}
}

func (cfg *config) waitmajority(seq int) {
	cfg.waitn(seq, (cfg.n/2)+1)
}

func (cfg *config) checkmax(seq int, max int) {
	nd := cfg.ndecided(seq)
	if nd > max {
		cfg.t.Fatalf("too many decided; seq=%v ndecided=%v max=%v", seq, nd, max)
	}
}

func (cfg *config) one(server, seq int, cmd interface{}) {
	cfg.pxa[server].Start(seq, cmd)
	cfg.waitn(seq, cfg.n)
}

func (cfg *config) partition(p [][]int) {
	for i := 0; i < cfg.n; i++ {
		cfg.disconnect(i)
	}
	for _, pi := range p {
		for _, pa := range pi {
			cfg.connected[pa] = true
			for _, pb := range pi {
				endname := cfg.endnames[pa][pb]
				cfg.net.Enable(endname, true)
				endname = cfg.endnames[pb][pa]
				cfg.net.Enable(endname, true)
			}
		}
	}
}
