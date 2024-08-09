package queue

import "testing"

func TestNew(t *testing.T) {
	q := New[int](3)
	if q.capacity != 3 {
		t.Errorf("expected capacity 3, got %d", q.capacity)
	}

	if len(q.data) != 0 {
		t.Errorf("expected empty queue, got %v", q.data)
	}
}

func TestQueue_Enqueue(t *testing.T) {
	q := New[int](3)
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)
	q.Enqueue(4)

	expected := []int{2, 3, 4}
	if len(q.data) != 3 {
		t.Errorf("expected queue with 3 elements, got %v", q.data)
	}

	for i, v := range q.data {
		if v != expected[i] {
			t.Errorf("expected %d, got %d", expected[i], v)
		}
	}
}

func TestQueue_Dequeue(t *testing.T) {
	q := New[int](3)
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)

	v, ok := q.Dequeue()
	if !ok {
		t.Errorf("expected true, got false")
	}

	if v != 1 {
		t.Errorf("expected 1, got %d", v)
	}

	expected := []int{2, 3}
	if len(q.data) != 2 {
		t.Errorf("expected queue with 2 elements, got %v", q.data)
	}

	for i, v := range q.data {
		if v != expected[i] {
			t.Errorf("expected %d, got %d", expected[i], v)
		}
	}
}

func TestQueue_Pop(t *testing.T) {
	q := New[int](3)
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)

	v, ok := q.Pop()
	if !ok {
		t.Errorf("expected true, got false")
	}

	if v != 3 {
		t.Errorf("expected 3, got %d", v)
	}

	expected := []int{1, 2}
	if len(q.data) != 2 {
		t.Errorf("expected queue with 2 elements, got %v", q.data)
	}

	for i, v := range q.data {
		if v != expected[i] {
			t.Errorf("expected %d, got %d", expected[i], v)
		}
	}
}

func TestQueue_Oldest(t *testing.T) {
	q := New[int](3)
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)

	v, ok := q.Oldest()
	if !ok {
		t.Errorf("expected true, got false")
	}

	if v != 1 {
		t.Errorf("expected 1, got %d", v)
	}

	expected := []int{1, 2, 3}
	if len(q.data) != 3 {
		t.Errorf("expected queue with 3 elements, got %v", q.data)
	}

	for i, v := range q.data {
		if v != expected[i] {
			t.Errorf("expected %d, got %d", expected[i], v)
		}
	}
}

func TestQueue_Newest(t *testing.T) {
	q := New[int](3)
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)

	v, ok := q.Newest()
	if !ok {
		t.Errorf("expected true, got false")
	}

	if v != 3 {
		t.Errorf("expected 3, got %d", v)
	}

	expected := []int{1, 2, 3}
	if len(q.data) != 3 {
		t.Errorf("expected queue with 3 elements, got %v", q.data)
	}

	for i, v := range q.data {
		if v != expected[i] {
			t.Errorf("expected %d, got %d", expected[i], v)
		}
	}
}

func TestQueue_Len(t *testing.T) {
	q := New[int](3)
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)

	if q.Len() != 3 {
		t.Errorf("expected 3, got %d", q.Len())
	}
}

func TestQueue_Contains(t *testing.T) {
	q := New[int](3)
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)

	if !q.Contains(func(i int) bool { return i == 2 }) {
		t.Errorf("expected true, got false")
	}

	if q.Contains(func(i int) bool { return i == 4 }) {
		t.Errorf("expected false, got true")
	}
}
