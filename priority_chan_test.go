package priority_chan

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Cond(t *testing.T) {
	testBase(t, 10, 100, 100000)
	testBase(t, 20, 100, 100000)
	testBase(t, 20, 20, 100000)
	testBase(t, 30, 50, 100000)
}

func Test_Mem(t *testing.T) {
	for idx := 0; idx < 4000; idx++ {
		testBase(t, rand.Intn(100)+3, rand.Intn(10)+4, rand.Intn(10000)*20)
	}
}

func testBase(t *testing.T, queueSize int, parallel int, count int) {
	pc := New(queueSize)
	wg1 := &sync.WaitGroup{}
	wg2 := &sync.WaitGroup{}
	step := count / parallel
	var sendHash, sendCnt, recvHash, recvCnt int64
	slock := sync.Mutex{}
	rlock := sync.Mutex{}
	wg1.Add(parallel)
	for idx := 0; idx < parallel; idx++ {
		go func() {
			defer wg1.Done()
			hash, cnt := genSender(t, pc, idx*step, (idx+1)*step)
			slock.Lock()
			defer slock.Unlock()
			sendHash ^= hash
			sendCnt += cnt
		}()
	}
	wg2.Add(32)
	for idx := 0; idx < 32; idx++ {
		go func() {
			defer wg2.Done()
			hash, cnt := genRecv(t, pc)
			rlock.Lock()
			defer rlock.Unlock()
			recvHash ^= hash
			recvCnt += cnt
		}()
	}
	wg1.Wait()
	pc.Close()
	wg2.Wait()
	assert.Equal(t, sendHash, recvHash)
	assert.Equal(t, sendCnt, recvCnt)
}

type Item struct {
	Value int
	Prio  int64
}

func (i *Item) Priority() uint64 {
	return uint64(i.Prio)
}
func genSender(t *testing.T, pc *PriorityChannel, begin, end int) (hash, cnt int64) {
	for i := begin; i < end; i++ {
		pc.Put(&Item{
			Value: i,
			Prio:  int64(i),
		})
		//t.Logf("send %d\n", i)
		hash ^= int64(i)
		cnt++
	}
	return
}

func genRecv(t *testing.T, pc *PriorityChannel) (hash, cnt int64) {
	swap := false
	_ = swap
	for {
		_v, ok := <-pc.Reader()
		if !ok {
			return
		}
		v := _v.(*Item)
		//t.Logf("recv %d\n", v.Priority)
		if v.Prio%10 == 0 && !swap {
			swap = true
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
			_v, _ = pc.Swap(ctx, _v)
			v = _v.(*Item)
			cancel()
			hash ^= v.Prio
			cnt++
			continue
		}
		swap = false
		hash ^= v.Prio
		cnt++
	}
	return
}