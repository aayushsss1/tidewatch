package agent

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type qItem struct {
	inc      *Incident
	score    float64
	deadline time.Time
	seq      int
}

type qHeap []*qItem

func (h qHeap) Len() int { return len(h) }
func (h qHeap) Less(i, j int) bool {
	if h[i].score != h[j].score {
		return h[i].score > h[j].score
	}
	return h[i].seq < h[j].seq
}
func (h qHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *qHeap) Push(x any)   { *h = append(*h, x.(*qItem)) }
func (h *qHeap) Pop() any {
	old := *h
	it := old[len(old)-1]
	*h = old[:len(old)-1]
	return it
}

// Queue is a blocking priority queue of incidents.
type Queue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	h       qHeap
	seq     int
	dropped []*Incident
}

// NewQueue returns an empty Queue.
func NewQueue() *Queue {
	q := &Queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Push enqueues an incident with its score and deadline.
func (q *Queue) Push(inc *Incident, score float64, deadline time.Time) {
	q.mu.Lock()
	q.seq++
	heap.Push(&q.h, &qItem{inc: inc, score: score, deadline: deadline, seq: q.seq})
	q.mu.Unlock()
	q.cond.Signal()
}

// Pop blocks for the highest-score incident whose deadline has not passed.
// Expired entries are moved to Dropped. It returns false when ctx is done.
func (q *Queue) Pop(ctx context.Context) (*Incident, time.Time, bool) {
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return nil, time.Time{}, false
		}
		for q.h.Len() > 0 {
			it := heap.Pop(&q.h).(*qItem)
			if !time.Now().Before(it.deadline) {
				q.dropped = append(q.dropped, it.inc)
				continue
			}
			return it.inc, it.deadline, true
		}
		q.cond.Wait()
	}
}

// Dropped returns and clears the incidents dropped for passing their deadline.
func (q *Queue) Dropped() []*Incident {
	q.mu.Lock()
	defer q.mu.Unlock()
	d := q.dropped
	q.dropped = nil
	return d
}

// Len returns the number of queued incidents.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.h.Len()
}
