package priority_chan

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrClosed = errors.New("PriorityChannel Closed")
)

// PriorityChannel as implemented by a min heap
// ie. the 0th element is the *lowest* value.
type PriorityChannel struct {
	hp    itemHeap
	In    chan<- Item //User need close In when done
	Out   <-chan Item
	inner atomic.Value
}

// New creates a PriorityChannel of the given capacity.
func New(capacity int) *PriorityChannel {
	pc := PriorityChannel{
		hp: make(itemHeap, 0, capacity+1),
	}
	in := make(chan Item)
	out := make(chan Item)
	inner := make(chan Item)
	pc.inner.Store(inner)
	mtx := sync.Mutex{}
	rctl := sync.NewCond(&mtx)
	wctl := sync.NewCond(&mtx)
	//returnCtl := sync.NewCond(&mtx)
	wgRoutine := sync.WaitGroup{}
	wgRoutine.Add(3)
	exited := int32(0)
	go func() {
		defer wgRoutine.Done()
		for {
			// 队列满等待
			mtx.Lock()
			for pc.Len() >= capacity {
				wctl.Wait()
			}
			mtx.Unlock()
			item, ok := <-in
			if !ok {
				atomic.StoreInt32(&exited, 1)
				return
			}

			mtx.Lock()
			heap.Push(&pc.hp, item)
			if pc.Len() == 1 {
				rctl.Signal()
			}
			mtx.Unlock()
		}
	}()

	go func() { // 消费端
		defer wgRoutine.Done()
		for {
			mtx.Lock()
			for pc.Len() == 0 {
				if atomic.LoadInt32(&exited) == 2 {
					mtx.Unlock()
					return
				}
				rctl.Wait()
			}
			item := heap.Pop(&pc.hp).(Item)
			if pc.Len() == capacity-1 {
				wctl.Signal()
			}
			mtx.Unlock()
			out <- item
		}
	}()

	go func() {
		defer wgRoutine.Done()
		var item Item
		for {
			select {
			case item = <-inner:
			case <-time.After(time.Second * 10):
				if atomic.LoadInt32(&exited) == 1 {
					pc.inner.Store((chan Item)(nil))
					atomic.StoreInt32(&exited, 2)
					rctl.Signal()
					return
				}
				continue
			}
			mtx.Lock()
			heap.Push(&pc.hp, item)
			if pc.Len() == 1 {
				rctl.Signal()
			}
			mtx.Unlock()
		}
	}()

	go func() {
		wgRoutine.Wait()
		close(out)
		close(inner)
	}()

	pc.In = in
	pc.Out = out
	return &pc
}

func (pc *PriorityChannel) Return(ctx context.Context, e Item) error {
	var inner chan Item
	for {
		if val := pc.inner.Load(); val == nil {
			return ErrClosed
		} else {
			inner = val.(chan Item)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inner <- e:
			return nil
		case <-time.After(time.Second):
			continue
		}
	}
}

// Len return length of priority queue
func (pc *PriorityChannel) Len() int {
	return pc.hp.Len()
}

// Item in the itemHeap.
type Item struct {
	Value    interface{}
	Priority int64
}

// itemHeap as implemented by a min heap
// ie. the 0th element is the *lowest* value.
type itemHeap []Item

// Len returns the length of the queue.
func (h itemHeap) Len() int {
	return len(h)
}

// Less returns true if the item at index i has a lower priority than the item
// at index j.
func (h itemHeap) Less(i, j int) bool {
	return h[i].Priority < h[j].Priority
}

// Swap the items at index i and j.
func (h itemHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push a new value to the queue.
func (h *itemHeap) Push(x interface{}) {
	*h = append(*h, x.(Item))
}

// Pop an item from the queue.
func (h *itemHeap) Pop() interface{} {
	n := len(*h)
	item := (*h)[n-1]
	*h = (*h)[0 : n-1]
	return item
}
