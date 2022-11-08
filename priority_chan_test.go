package priority_chan

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Cond(t *testing.T) {
	testBase(t, 10, 100, 10000000)
	testBase(t, 20, 100, 10000000)
	testBase(t, 20, 20, 10000000)
	testBase(t, 30, 50, 10000000)
}

func Test_Mem(t *testing.T) {
	for idx := 0; idx < 1000; idx++ {
		testBase(t, 100, 10, 10000)
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
	close(pc.In)
	wg2.Wait()
	assert.Equal(t, sendHash, recvHash)
	assert.Equal(t, sendCnt, recvCnt)
}

func genSender(t *testing.T, pc *PriorityChannel, begin, end int) (hash, cnt int64) {
	for i := begin; i < end; i++ {
		pc.In <- Item{
			Value:    i,
			Priority: int64(i),
		}
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
		v, ok := <-pc.Out
		if !ok {
			return
		}
		//t.Logf("recv %d\n", v.Priority)
		if v.Priority%10 == 0 && !swap {
			swap = true
			pc.Return(context.Background(), v)
			continue
		}
		swap = false
		hash ^= v.Priority
		cnt++
	}
	return
}
