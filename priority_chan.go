package priority_chan

import (
"container/heap"
"context"
"errors"
"sync"
)

var (
	ErrClosed = errors.New("PriorityChannel Closed")
)

type Prioritor interface {
	Priority() uint64
}

// PriorityChannel as implemented by a min heap
// ie. the 0th element is the *lowest* value.
type PriorityChannel struct {
	sync.RWMutex
	hp       prioritorHeap
	writerCh chan Prioritor //User need close In when done
	readerCh chan Prioritor
	swapin   chan Prioritor
	swapout  chan Prioritor
	exitFlag bool
	exitch   chan struct{}
}

func (pc *PriorityChannel) Reader() <-chan Prioritor {
	return pc.readerCh
}
func (pc *PriorityChannel) Put(val Prioritor) error {
	pc.RLock()
	defer pc.RUnlock()
	if pc.exitFlag == true {
		return ErrClosed
	}
	pc.writerCh <- val
	return nil
}

// New creates a PriorityChannel of the given capacity.
func New(capacity int) *PriorityChannel {
	pc := PriorityChannel{
		hp:       make(prioritorHeap, 0, capacity+1),
		writerCh: make(chan Prioritor),
		readerCh: make(chan Prioritor),
		swapin:   make(chan Prioritor),
		swapout:  make(chan Prioritor),
		exitch:   make(chan struct{}),
	}
	go func() {
		defer func() {
			close(pc.writerCh)
			close(pc.readerCh)
		}()
		var prev Prioritor
		changeItem := func(recv Prioritor, prev Prioritor) Prioritor {
			heap.Push(&pc.hp, recv)
			if prev != nil {
				heap.Push(&pc.hp, prev)
			}
			return heap.Pop(&pc.hp).(Prioritor)
		}
	EXIT:
		for {
			writer := pc.writerCh
			reader := pc.readerCh
			exit := pc.exitch
			if len(pc.hp) >= cap(pc.hp) {
				writer = nil
			} else if pc.hp.Len() == 0 && prev == nil {
				reader = nil
			}
			if pc.exitFlag {
				writer = nil
				if pc.hp.Len() == 0 && prev == nil { // read finish
					break EXIT
				}
				exit = nil
			}
			if prev == nil && pc.hp.Len() > 0 {
				prev = heap.Pop(&pc.hp).(Prioritor)
			}

			select {
			case item := <-writer:
				prev = changeItem(item, prev)
			case reader <- prev:
				prev = nil
			case item := <-pc.swapin:
				pc.swapout <- changeItem(item, prev)
				prev = nil
			case <-exit:
				func() {
					pc.Lock()
					defer pc.Unlock()
					pc.exitFlag = true
					close(pc.swapin)
					close(pc.swapout)
					pc.swapin = nil
					pc.swapout = nil
				}()
			}
		}
	}()
	return &pc
}

func (pc *PriorityChannel) Swap(ctx context.Context, e Prioritor) (Prioritor, error) {
	pc.RLock()
	defer pc.RUnlock()
	if pc.exitFlag {
		return e, ErrClosed
	}
	select {
	case <-ctx.Done():
		return e, ctx.Err()
	case pc.swapin <- e:
		t := <-pc.swapout
		return t, nil
	}
}

func (pc *PriorityChannel) Close() {
	pc.Lock()
	defer pc.Unlock()
	close(pc.exitch)
}

// Len return length of priority queue
func (pc *PriorityChannel) Len() int {
	return pc.hp.Len()
}

// prioritorHeap as implemented by a min heap
// ie. the 0th element is the *lowest* value.
type prioritorHeap []Prioritor

// Len returns the length of the queue.
func (h prioritorHeap) Len() int {
	return len(h)
}

// Less returns true if the item at index i has a lower priority than the item
// at index j.
func (h prioritorHeap) Less(i, j int) bool {
	return h[i].Priority() < h[j].Priority()
}

// Swap the items at index i and j.
func (h prioritorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push a new value to the queue.
func (h *prioritorHeap) Push(x interface{}) {
	*h = append(*h, x.(Prioritor))
}

// Pop an item from the queue.
func (h *prioritorHeap) Pop() interface{} {
	n := len(*h)
	item := (*h)[n-1]
	*h = (*h)[0 : n-1]
	return item
}