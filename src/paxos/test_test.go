package paxos

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestSpeed(t *testing.T) {
	npaxos := 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup(3)

	cfg.begin("Test: Speed")

	fmt.Printf("Test: 200 agreements sequential ...\n")
	st := time.Now()
	for i := 0; i < 200; i++ {
		cfg.one(0, i, "seq")
	}
	fmt.Printf("  ... Using %v ms\n", time.Since(st).Milliseconds())

	fmt.Printf("Test: 200 agreements concurrent ...\n")
	st = time.Now()
	for i := 0; i < 200; i++ {
		cfg.pxa[0].Start(200+i, "con")
	}
	for i := 0; i < 200; i++ {
		cfg.waitn(200+i, npaxos)
	}
	fmt.Printf("  ... Using %v ms\n", time.Since(st).Milliseconds())

	cfg.end()
}

func TestBasic(t *testing.T) {
	npaxos := 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup(3)

	cfg.begin("Test: Basic")
	fmt.Printf("Test: Single proposer ...\n")

	cfg.one(0, 0, "hello")

	fmt.Printf("  ... Passed\n")
	fmt.Printf("Test: Many proposers, same value ...\n")

	for i := 0; i < npaxos; i++ {
		cfg.pxa[i].Start(1, 77)
	}
	cfg.waitn(1, npaxos)

	fmt.Printf("  ... Passed\n")
	fmt.Printf("Test: Many proposers, different values ...\n")

	for i := 0; i < npaxos; i++ {
		cfg.pxa[i].Start(2, 100+i)
	}
	cfg.waitn(2, npaxos)

	fmt.Printf("  ... Passed\n")
	fmt.Printf("Test: Out-of-order instances ...\n")

	cfg.pxa[0].Start(7, 700)
	cfg.pxa[0].Start(6, 600)
	cfg.pxa[1].Start(5, 500)
	cfg.waitn(7, npaxos)
	cfg.pxa[0].Start(4, 400)
	cfg.pxa[1].Start(3, 300)
	cfg.waitn(6, npaxos)
	cfg.waitn(5, npaxos)
	cfg.waitn(4, npaxos)
	cfg.waitn(3, npaxos)

	if cfg.pxa[0].Max() != 7 {
		t.Fatalf("wrong Max()")
	}
	fmt.Printf("  ... Passed\n")

	cfg.end()
}

func TestDeaf(t *testing.T) {

	npaxos := 5
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup(8)

	cfg.begin("Test: Deaf proposer")

	cfg.one(0, 0, "hello")

	cfg.disconnectIncome(0)
	cfg.disconnectIncome(npaxos - 1)

	cfg.pxa[1].Start(1, "goodbye")
	cfg.waitmajority(1)
	cfg.checkmax(1, npaxos-2)

	cfg.pxa[0].Start(1, "xxx")
	cfg.waitn(1, npaxos-1)
	cfg.checkmax(1, npaxos-1)

	cfg.one(npaxos-1, 1, "yyy")

	cfg.end()
}

func TestForget(t *testing.T) {
	npaxos := 6
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup(3)

	cfg.begin("Test: Forgetting")

	// initial Min() correct?
	for i := 0; i < npaxos; i++ {
		m := cfg.pxa[i].Min()
		if m > 0 {
			t.Fatalf("wrong initial Min() %v", m)
		}
	}

	cfg.pxa[0].Start(0, "00")
	cfg.pxa[1].Start(1, "11")
	cfg.pxa[2].Start(2, "22")
	cfg.pxa[0].Start(6, "66")
	cfg.pxa[1].Start(7, "77")

	cfg.waitn(0, npaxos)

	// Min() correct?
	for i := 0; i < npaxos; i++ {
		m := cfg.pxa[i].Min()
		if m != 0 {
			t.Fatalf("wrong Min() %v; expected 0", m)
		}
	}

	cfg.waitn(1, npaxos)

	// Min() correct?
	for i := 0; i < npaxos; i++ {
		m := cfg.pxa[i].Min()
		if m != 0 {
			t.Fatalf("wrong Min() %v; expected 0", m)
		}
	}

	// everyone Done() -> Min() changes?
	for i := 0; i < npaxos; i++ {
		cfg.pxa[i].Done(0)
	}
	for i := 1; i < npaxos; i++ {
		cfg.pxa[i].Done(1)
	}
	for i := 0; i < npaxos; i++ {
		cfg.pxa[i].Start(8+i, "xx")
	}
	allok := false
	for iters := 0; iters < 12; iters++ {
		allok = true
		for i := 0; i < npaxos; i++ {
			s := cfg.pxa[i].Min()
			if s != 1 {
				allok = false
			}
		}
		if allok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if allok != true {
		t.Fatalf("Min() did not advance after Done()")
	}

	cfg.end()
}

func TestManyForget(t *testing.T) {
	npaxos := 3
	cfg := make_config(t, npaxos, true)
	defer cfg.cleanup(3)

	cfg.begin("Test: Lots of forgetting")

	const maxseq = 20

	go func() {
		na := rand.Perm(maxseq)
		for i := 0; i < len(na); i++ {
			seq := na[i]
			j := (rand.Int() % npaxos)
			v := rand.Int()
			cfg.pxa[j].Start(seq, v)
			runtime.Gosched()
		}
	}()

	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			seq := (rand.Int() % maxseq)
			i := (rand.Int() % npaxos)
			if seq >= cfg.pxa[i].Min() {
				decided, _ := cfg.pxa[i].Status(seq)
				if decided == Decided {
					cfg.pxa[i].Done(seq)
				}
			}
			runtime.Gosched()
		}
	}()

	time.Sleep(5 * time.Second)
	done <- true
	cfg.setunreliable(false)
	time.Sleep(2 * time.Second)

	for seq := 0; seq < maxseq; seq++ {
		for i := 0; i < npaxos; i++ {
			if seq >= cfg.pxa[i].Min() {
				cfg.pxa[i].Status(seq)
			}
		}
	}

	cfg.end()
}

// does paxos forgetting actually free the memory?
func TestForgetMem(t *testing.T) {
	npaxos := 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup(3)

	cfg.begin("Test: Paxos frees forgotten instance memory")

	cfg.one(0, 0, "x")

	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	// m0.Alloc about a megabyte

	for i := 1; i <= 10; i++ {
		big := make([]byte, 1000000)
		for j := 0; j < len(big); j++ {
			big[j] = byte('a' + rand.Int()%26)
		}
		cfg.one(0, i, string(big))
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	// m1.Alloc about 90 megabytes

	for i := 0; i < npaxos; i++ {
		cfg.pxa[i].Done(10)
	}
	for i := 0; i < npaxos; i++ {
		cfg.pxa[i].Start(11+i, "z")
	}
	time.Sleep(10 * time.Second)
	for i := 0; i < npaxos; i++ {
		if cfg.pxa[i].Min() != 11 {
			t.Fatalf("expected Min() %v, got %v\n", 11, cfg.pxa[i].Min())
		}
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	// m2.Alloc about 10 megabytes

	if m2.Alloc > (m1.Alloc / 2) {
		t.Fatalf("memory use did not shrink enough")
	}

	again := make([]string, 10)
	for seq := 0; seq < npaxos && seq < 10; seq++ {
		again[seq] = randstring(20)
		for i := 0; i < npaxos; i++ {
			fate, _ := cfg.pxa[i].Status(seq)
			if fate != Forgotten {
				t.Fatalf("seq %d < Min() %d but not Forgotten", seq, cfg.pxa[i].Min())
			}
			cfg.pxa[i].Start(seq, again[seq])
		}
	}
	time.Sleep(1 * time.Second)
	for seq := 0; seq < npaxos && seq < 10; seq++ {
		for i := 0; i < npaxos; i++ {
			fate, v := cfg.pxa[i].Status(seq)
			if fate != Forgotten || v == again[seq] {
				t.Fatalf("seq %d < Min() %d but not Forgotten", seq, cfg.pxa[i].Min())
			}
		}
	}

	cfg.end()
}

// does Max() work after Done()s?
func TestDoneMax(t *testing.T) {
	npaxos := 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup(3)

	cfg.begin("Test: Paxos Max() after Done()s")

	cfg.one(0, 0, "x")

	for i := 1; i <= 10; i++ {
		cfg.one(0, i, "y")
	}

	for i := 0; i < npaxos; i++ {
		cfg.pxa[i].Done(10)
	}

	// Propagate messages so everyone knows about Done(10)
	for i := 0; i < npaxos; i++ {
		cfg.pxa[i].Start(10, "z")
	}
	time.Sleep(2 * time.Second)
	for i := 0; i < npaxos; i++ {
		mx := cfg.pxa[i].Max()
		if mx != 10 {
			t.Fatalf("Max() did not return correct result %d after calling Done(); returned %d", 10, mx)
		}
	}

	cfg.end()
}

func TestRPCCount(t *testing.T) {
	npaxos := 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup(3)

	cfg.begin("Test: RPC counts aren't too high")

	ninst1 := 5
	seq := 0
	for i := 0; i < ninst1; i++ {
		cfg.one(0, seq, "x")
		seq++
	}

	time.Sleep(2 * time.Second)

	total1 := -cfg.rpcs0
	for j := 0; j < npaxos; j++ {
		total1 += cfg.rpcCount(j)
	}
	t1 := time.Now()

	// per agreement:
	// 3 prepares
	// 3 accepts
	// 3 decides
	expected1 := ninst1*npaxos*3 + int(t1.Sub(cfg.t0)/time.Millisecond/tickerIntv)*npaxos*npaxos
	if total1 > expected1 {
		t.Fatalf("too many RPCs for serial Start()s; %v instances, got %v, expected %v",
			ninst1, total1, expected1)
	}

	ninst2 := 5
	for i := 0; i < ninst2; i++ {
		for j := 0; j < npaxos; j++ {
			go cfg.pxa[j].Start(seq, j+(i*10))
		}
		cfg.waitn(seq, npaxos)
		seq++
	}

	time.Sleep(2 * time.Second)

	total2 := -total1
	for j := 0; j < npaxos; j++ {
		total2 += cfg.rpcCount(j)
	}
	t2 := time.Now()

	// worst case per agreement:
	// Proposer 1: 3 prep, 3 acc, 3 decides.
	// Proposer 2: 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
	// Proposer 3: 3 prep, 3 acc, 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
	expected2 := ninst2*npaxos*15 + int(t2.Sub(t1)/time.Millisecond/tickerIntv)*npaxos*npaxos
	if total2 > expected2 {
		t.Fatalf("too many RPCs for concurrent Start()s; %v instances, got %v, expected %v",
			ninst2, total2, expected2)
	}

	cfg.end()
}

// many agreements (without failures)
func TestMany(t *testing.T) {
	npaxos := 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup(3)

	cfg.begin("Test: Many instances")

	for i := 0; i < npaxos; i++ {
		cfg.pxa[i].Start(0, 0)
	}

	const ninst = 50
	for seq := 1; seq < ninst; seq++ {
		// only 5 active instances, to limit the
		// number of file descriptors.
		for seq >= 5 && cfg.ndecided(seq-5) < npaxos {
			time.Sleep(20 * time.Millisecond)
		}
		for i := 0; i < npaxos; i++ {
			cfg.pxa[i].Start(seq, (seq*10)+i)
		}
	}

	for {
		done := true
		for seq := 1; seq < ninst; seq++ {
			if cfg.ndecided(seq) < npaxos {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cfg.end()
}

// a peer starts up, with proposal, after others decide.
// then another peer starts, without a proposal.
func TestOld(t *testing.T) {
	npaxos := 5
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup(5)

	cfg.begin("Test: Minority proposal ignored")

	cfg.crash1(0)
	cfg.crash1(4)
	cfg.pxa[1].Start(1, 111)

	cfg.waitmajority(1)

	cfg.start1(0)
	cfg.connect(0)
	cfg.pxa[0].Start(1, 222)

	cfg.waitn(1, 4)

	cfg.start1(4)
	cfg.connect(4)
	cfg.waitn(1, npaxos)

	cfg.end()
}

// many agreements, with unreliable RPC
func TestManyUnreliable(t *testing.T) {
	npaxos := 3
	cfg := make_config(t, npaxos, true)
	defer cfg.cleanup(3)

	cfg.begin("Test: Many instances, unreliable RPC")

	for i := 0; i < npaxos; i++ {
		cfg.pxa[i].Start(0, 0)
	}

	const ninst = 50
	for seq := 1; seq < ninst; seq++ {
		// only 3 active instances, to limit the
		// number of file descriptors.
		for seq >= 3 && cfg.ndecided(seq-3) < npaxos {
			time.Sleep(20 * time.Millisecond)
		}
		for i := 0; i < npaxos; i++ {
			cfg.pxa[i].Start(seq, (seq*10)+i)
		}
	}

	for {
		done := true
		for seq := 1; seq < ninst; seq++ {
			if cfg.ndecided(seq) < npaxos {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cfg.end()
}

func TestPartition(t *testing.T) {
	npaxos := 5
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup(10)

	seq := 0

	cfg.begin("Test: Partition")

	fmt.Printf("Test: No decision if partitioned ...\n")
	cfg.partition([][]int{{0, 2}, {1, 3}, {4}})

	cfg.pxa[1].Start(seq, 111)
	time.Sleep(3 * time.Second)
	cfg.checkmax(seq, 0)

	fmt.Printf("  ... Passed\n")
	fmt.Printf("Test: Decision in majority partition ...\n")

	cfg.partition([][]int{{0}, {1, 2, 3}, {4}})

	time.Sleep(2 * time.Second)
	cfg.waitmajority(seq)

	fmt.Printf("  ... Passed\n")
	fmt.Printf("Test: All agree after full heal ...\n")

	cfg.pxa[0].Start(seq, 1000) // poke them
	cfg.pxa[4].Start(seq, 1004)
	cfg.partition([][]int{{0, 1, 2, 3, 4}})

	cfg.waitn(seq, npaxos)

	fmt.Printf("  ... Passed\n")
	cfg.setLongDelay(false)
	fmt.Printf("Test: One peer switches partitions ...\n")

	for iters := 0; iters < 20; iters++ {
		seq++

		cfg.partition([][]int{{0, 1, 2}, {3, 4}})
		cfg.pxa[0].Start(seq, seq*10)
		cfg.pxa[3].Start(seq, (seq*10)+1)
		cfg.waitmajority(seq)
		cfg.checkmax(seq, 3)

		cfg.partition([][]int{{0, 1}, {2, 3, 4}})
		cfg.waitn(seq, npaxos)
	}

	fmt.Printf("  ... Passed\n")
	fmt.Printf("Test: One peer switches partitions, unreliable ...\n")

	for iters := 0; iters < 20; iters++ {
		seq++

		cfg.setunreliable(true)
		cfg.partition([][]int{{0, 1, 2}, {3, 4}})
		for i := 0; i < npaxos; i++ {
			cfg.pxa[i].Start(seq, (seq*10)+i)
		}
		cfg.waitn(seq, 3)
		cfg.checkmax(seq, 3)

		cfg.partition([][]int{{0, 1}, {2, 3, 4}})

		cfg.setunreliable(false)

		cfg.waitn(seq, 5)
	}

	fmt.Printf("  ... Passed\n")
	cfg.end()
}

func TestLots(t *testing.T) {
	const npaxos = 5
	cfg := make_config(t, npaxos, true)
	defer cfg.cleanup(10)

	cfg.begin("Test: Many requests, changing partitions")

	done := int32(0)

	// re-partition periodically
	ch1 := make(chan bool)
	go func() {
		defer func() { ch1 <- true }()
		for atomic.LoadInt32(&done) == 0 {
			var a [npaxos]int
			for i := 0; i < npaxos; i++ {
				a[i] = (rand.Int() % 3)
			}
			pa := make([][]int, 3)
			for i := 0; i < 3; i++ {
				pa[i] = make([]int, 0)
				for j := 0; j < npaxos; j++ {
					if a[j] == i {
						pa[i] = append(pa[i], j)
					}
				}
			}
			cfg.partition(pa)
			time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
		}
	}()

	seq := int32(0)

	// periodically start a new instance
	ch2 := make(chan bool)
	go func() {
		defer func() { ch2 <- true }()
		for atomic.LoadInt32(&done) == 0 {
			// how many instances are in progress?
			nd := 0
			sq := int(atomic.LoadInt32(&seq))
			for i := 0; i < sq; i++ {
				if cfg.ndecided(i) == npaxos {
					nd++
				}
			}
			if sq-nd < 10 {
				for i := 0; i < npaxos; i++ {
					cfg.pxa[i].Start(sq, rand.Int()%10)
				}
				atomic.AddInt32(&seq, 1)
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	// periodically check that decisions are consistent
	ch3 := make(chan bool)
	go func() {
		defer func() { ch3 <- true }()
		for atomic.LoadInt32(&done) == 0 {
			for i := 0; i < int(atomic.LoadInt32(&seq)); i++ {
				cfg.ndecided(i)
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	time.Sleep(200 * time.Second)
	atomic.StoreInt32(&done, 1)
	<-ch1
	<-ch2
	<-ch3

	// repair, then check that all instances decided.
	cfg.setunreliable(false)
	cfg.setLongDelay(false)
	cfg.partition([][]int{{0, 1, 2, 3, 4}})
	time.Sleep(5 * time.Second)

	for i := 0; i < int(atomic.LoadInt32(&seq)); i++ {
		cfg.waitn(i, npaxos)
	}

	cfg.end()
}
