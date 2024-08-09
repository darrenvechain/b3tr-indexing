package queue

// Queue represents a fixed-size FIFO queue with a generic type.
type Queue[T any] struct {
	data     []T
	capacity int
}

// New creates a new queue with a given capacity.
func New[T any](capacity int) *Queue[T] {
	return &Queue[T]{
		data:     make([]T, 0, capacity),
		capacity: capacity,
	}
}

// Enqueue adds a new element to the queue.
func (q *Queue[T]) Enqueue(element T) {
	if len(q.data) == q.capacity {
		// Remove the oldest element to make room for the new one.
		q.data = q.data[1:]
	}
	q.data = append(q.data, element)
}

// Dequeue removes and returns the oldest element from the queue.
func (q *Queue[T]) Dequeue() (T, bool) {
	if len(q.data) == 0 {
		var zeroValue T
		return zeroValue, false
	}
	element := q.data[0]
	q.data = q.data[1:]
	return element, true
}

// Pop removes and returns the newest element from the queue.
func (q *Queue[T]) Pop() (T, bool) {
	if len(q.data) == 0 {
		var zeroValue T
		return zeroValue, false
	}
	element := q.data[len(q.data)-1]
	q.data = q.data[:len(q.data)-1]
	return element, true
}

// Oldest returns the oldest element without removing it from the queue.
func (q *Queue[T]) Oldest() (T, bool) {
	if len(q.data) == 0 {
		var zeroValue T
		return zeroValue, false
	}
	return q.data[0], true
}

// Newest returns the newest element without removing it from the queue.
func (q *Queue[T]) Newest() (T, bool) {
	if len(q.data) == 0 {
		var zeroValue T
		return zeroValue, false
	}
	return q.data[len(q.data)-1], true
}

// Len returns the number of elements currently in the queue.
func (q *Queue[T]) Len() int {
	return len(q.data)
}

// Contains accepts a callback to check if an element is in the queue.
func (q *Queue[T]) Contains(matcher func(T) bool) bool {
	for _, element := range q.data {
		if matcher(element) {
			return true
		}
	}
	return false
}
