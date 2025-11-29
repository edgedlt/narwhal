package narwhal

import (
	"sync/atomic"
	"time"
)

// MeteredChannel wraps a channel with metrics for observability.
// Tracks sent, received, dropped counts and queue depth. Thread-safe.
type MeteredChannel[T any] struct {
	ch   chan T
	name string

	// Counters
	sent     atomic.Uint64
	received atomic.Uint64
	dropped  atomic.Uint64

	// Timing
	createdAt time.Time
}

// NewMeteredChannel creates a new metered channel with the given name and capacity.
func NewMeteredChannel[T any](name string, capacity int) *MeteredChannel[T] {
	return &MeteredChannel[T]{
		ch:        make(chan T, capacity),
		name:      name,
		createdAt: time.Now(),
	}
}

// Send sends a value on the channel.
// Blocks if the channel is full.
func (m *MeteredChannel[T]) Send(value T) {
	m.ch <- value
	m.sent.Add(1)
}

// TrySend attempts to send a value on the channel without blocking.
// Returns true if the send succeeded, false if the channel was full.
func (m *MeteredChannel[T]) TrySend(value T) bool {
	select {
	case m.ch <- value:
		m.sent.Add(1)
		return true
	default:
		m.dropped.Add(1)
		return false
	}
}

// Receive receives a value from the channel.
// Blocks if the channel is empty.
func (m *MeteredChannel[T]) Receive() T {
	value := <-m.ch
	m.received.Add(1)
	return value
}

// TryReceive attempts to receive a value from the channel without blocking.
// Returns the value and true if successful, or zero value and false if empty.
func (m *MeteredChannel[T]) TryReceive() (T, bool) {
	select {
	case value := <-m.ch:
		m.received.Add(1)
		return value, true
	default:
		var zero T
		return zero, false
	}
}

// ReceiveChan returns the underlying channel for use in select statements.
// Note: messages received directly from this channel won't update the received counter.
// Use MarkReceived() after receiving to update the counter.
func (m *MeteredChannel[T]) ReceiveChan() <-chan T {
	return m.ch
}

// MarkReceived increments the received counter.
// Call this after receiving from ReceiveChan() to maintain accurate metrics.
func (m *MeteredChannel[T]) MarkReceived() {
	m.received.Add(1)
}

// Len returns the current number of items in the channel.
func (m *MeteredChannel[T]) Len() int {
	return len(m.ch)
}

// Cap returns the capacity of the channel.
func (m *MeteredChannel[T]) Cap() int {
	return cap(m.ch)
}

// Close closes the underlying channel.
func (m *MeteredChannel[T]) Close() {
	close(m.ch)
}

// MeteredChannelStats contains statistics for a metered channel.
type MeteredChannelStats struct {
	// Name is the channel name.
	Name string

	// Sent is the total number of messages sent.
	Sent uint64

	// Received is the total number of messages received.
	Received uint64

	// Dropped is the total number of messages dropped due to full channel.
	Dropped uint64

	// Pending is the current number of messages in the channel.
	Pending int

	// Capacity is the channel capacity.
	Capacity int

	// Throughput is the approximate messages per second (sent).
	Throughput float64

	// Uptime is how long the channel has been running.
	Uptime time.Duration
}

// Stats returns current statistics.
func (m *MeteredChannel[T]) Stats() MeteredChannelStats {
	uptime := time.Since(m.createdAt)
	sent := m.sent.Load()

	var throughput float64
	if uptime.Seconds() > 0 {
		throughput = float64(sent) / uptime.Seconds()
	}

	return MeteredChannelStats{
		Name:       m.name,
		Sent:       sent,
		Received:   m.received.Load(),
		Dropped:    m.dropped.Load(),
		Pending:    len(m.ch),
		Capacity:   cap(m.ch),
		Throughput: throughput,
		Uptime:     uptime,
	}
}

// ChannelMetrics collects metrics from multiple metered channels.
// Useful for exposing all channel metrics through a single interface.
type ChannelMetrics struct {
	channels map[string]interface{ Stats() MeteredChannelStats }
}

// NewChannelMetrics creates a new ChannelMetrics collector.
func NewChannelMetrics() *ChannelMetrics {
	return &ChannelMetrics{
		channels: make(map[string]interface{ Stats() MeteredChannelStats }),
	}
}

// Register adds a metered channel to the collector.
func (cm *ChannelMetrics) Register(name string, ch interface{ Stats() MeteredChannelStats }) {
	cm.channels[name] = ch
}

// Unregister removes a channel from the collector.
func (cm *ChannelMetrics) Unregister(name string) {
	delete(cm.channels, name)
}

// AllStats returns statistics for all registered channels.
func (cm *ChannelMetrics) AllStats() map[string]MeteredChannelStats {
	stats := make(map[string]MeteredChannelStats, len(cm.channels))
	for name, ch := range cm.channels {
		stats[name] = ch.Stats()
	}
	return stats
}

// TotalPending returns the sum of pending messages across all channels.
func (cm *ChannelMetrics) TotalPending() int {
	total := 0
	for _, ch := range cm.channels {
		total += ch.Stats().Pending
	}
	return total
}

// TotalDropped returns the sum of dropped messages across all channels.
func (cm *ChannelMetrics) TotalDropped() uint64 {
	var total uint64
	for _, ch := range cm.channels {
		total += ch.Stats().Dropped
	}
	return total
}
