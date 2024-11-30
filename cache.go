package pubsub

import "sync"

type cache[K comparable, V any] struct {
	mutex sync.RWMutex
	items map[K]V
}

func newCache[K comparable, V any]() *cache[K, V] {
	return &cache[K, V]{
		items: make(map[K]V),
	}
}

func (c *cache[K, V]) get(key K) (V, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	v, ok := c.items[key]
	return v, ok
}

func (c *cache[K, V]) set(key K, value V) {
	c.mutex.RLock()
	if _, ok := c.items[key]; ok {
		c.mutex.RUnlock()
		return
	}
	c.mutex.RUnlock()

	c.mutex.Lock()
	c.items[key] = value
	c.mutex.Unlock()
}
