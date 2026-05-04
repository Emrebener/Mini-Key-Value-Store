package store

// lruList is an intrusive doubly-linked list over storedItems. The
// shard uses it as the recency tracker for eviction: pushFront on
// insert and refresh, remove on delete, back as the eviction
// candidate. Intrusive avoids the per-node allocation that
// container/list.PushFront causes, and lets MoveToFront and Remove
// run as direct pointer rewrites with no element-pool indirection.
//
// All methods are unsafe for concurrent use; the shard mutex is the
// caller's responsibility.
type lruList struct {
	head *storedItem
	tail *storedItem
}

func (l *lruList) pushFront(item *storedItem) {
	item.prev = nil
	item.next = l.head
	if l.head != nil {
		l.head.prev = item
	}
	l.head = item
	if l.tail == nil {
		l.tail = item
	}
}

func (l *lruList) remove(item *storedItem) {
	if item.prev != nil {
		item.prev.next = item.next
	} else {
		l.head = item.next
	}
	if item.next != nil {
		item.next.prev = item.prev
	} else {
		l.tail = item.prev
	}
	item.prev = nil
	item.next = nil
}

func (l *lruList) moveToFront(item *storedItem) {
	if l.head == item {
		return
	}
	l.remove(item)
	l.pushFront(item)
}

func (l *lruList) back() *storedItem {
	return l.tail
}
