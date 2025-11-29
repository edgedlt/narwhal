package narwhal

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMeteredChannel_NewMeteredChannel(t *testing.T) {
	ch := NewMeteredChannel[int]("test", 10)

	require.NotNil(t, ch)
	assert.Equal(t, "test", ch.name)
	assert.Equal(t, 10, ch.Cap())
	assert.Equal(t, 0, ch.Len())
}

func TestMeteredChannel_SendReceive(t *testing.T) {
	ch := NewMeteredChannel[int]("test", 10)

	// Send values
	ch.Send(1)
	ch.Send(2)
	ch.Send(3)

	assert.Equal(t, 3, ch.Len())

	// Receive values
	assert.Equal(t, 1, ch.Receive())
	assert.Equal(t, 2, ch.Receive())
	assert.Equal(t, 3, ch.Receive())

	assert.Equal(t, 0, ch.Len())

	stats := ch.Stats()
	assert.Equal(t, uint64(3), stats.Sent)
	assert.Equal(t, uint64(3), stats.Received)
	assert.Equal(t, uint64(0), stats.Dropped)
}

func TestMeteredChannel_TrySend(t *testing.T) {
	ch := NewMeteredChannel[int]("test", 2)

	// TrySend should succeed when not full
	assert.True(t, ch.TrySend(1))
	assert.True(t, ch.TrySend(2))

	// TrySend should fail when full
	assert.False(t, ch.TrySend(3))

	stats := ch.Stats()
	assert.Equal(t, uint64(2), stats.Sent)
	assert.Equal(t, uint64(1), stats.Dropped)
}

func TestMeteredChannel_TryReceive(t *testing.T) {
	ch := NewMeteredChannel[int]("test", 10)

	// TryReceive on empty channel
	val, ok := ch.TryReceive()
	assert.False(t, ok)
	assert.Equal(t, 0, val) // Zero value

	// Send and TryReceive
	ch.Send(42)
	val, ok = ch.TryReceive()
	assert.True(t, ok)
	assert.Equal(t, 42, val)

	stats := ch.Stats()
	assert.Equal(t, uint64(1), stats.Received)
}

func TestMeteredChannel_ReceiveChan(t *testing.T) {
	ch := NewMeteredChannel[int]("test", 10)

	ch.Send(1)
	ch.Send(2)

	// Use ReceiveChan in select
	select {
	case val := <-ch.ReceiveChan():
		assert.Equal(t, 1, val)
		ch.MarkReceived() // Must call manually
	default:
		t.Fatal("expected value from channel")
	}

	stats := ch.Stats()
	assert.Equal(t, uint64(1), stats.Received)
}

func TestMeteredChannel_MarkReceived(t *testing.T) {
	ch := NewMeteredChannel[int]("test", 10)

	ch.MarkReceived()
	ch.MarkReceived()
	ch.MarkReceived()

	stats := ch.Stats()
	assert.Equal(t, uint64(3), stats.Received)
}

func TestMeteredChannel_LenCap(t *testing.T) {
	ch := NewMeteredChannel[int]("test", 5)

	assert.Equal(t, 0, ch.Len())
	assert.Equal(t, 5, ch.Cap())

	ch.Send(1)
	ch.Send(2)

	assert.Equal(t, 2, ch.Len())
	assert.Equal(t, 5, ch.Cap())
}

func TestMeteredChannel_Close(t *testing.T) {
	ch := NewMeteredChannel[int]("test", 10)

	ch.Send(1)
	ch.Close()

	// Reading from closed channel with data should work
	val := ch.Receive()
	assert.Equal(t, 1, val)

	// Reading from closed empty channel returns zero value
	val, ok := <-ch.ReceiveChan()
	assert.False(t, ok)
	assert.Equal(t, 0, val)
}

func TestMeteredChannel_Stats(t *testing.T) {
	ch := NewMeteredChannel[int]("test-channel", 10)

	// Wait a tiny bit for uptime
	time.Sleep(10 * time.Millisecond)

	ch.Send(1)
	ch.Send(2)
	ch.TrySend(3)
	ch.Receive()

	stats := ch.Stats()

	assert.Equal(t, "test-channel", stats.Name)
	assert.Equal(t, uint64(3), stats.Sent)
	assert.Equal(t, uint64(1), stats.Received)
	assert.Equal(t, uint64(0), stats.Dropped)
	assert.Equal(t, 2, stats.Pending)
	assert.Equal(t, 10, stats.Capacity)
	assert.True(t, stats.Uptime >= 10*time.Millisecond)
	assert.True(t, stats.Throughput > 0) // Should have some throughput
}

func TestMeteredChannel_Throughput(t *testing.T) {
	ch := NewMeteredChannel[int]("test", 100)

	// Send many messages quickly
	for i := 0; i < 50; i++ {
		ch.Send(i)
	}

	time.Sleep(50 * time.Millisecond)

	stats := ch.Stats()
	// Throughput should be roughly 50 / 0.05 = 1000 msgs/sec, but timing varies
	assert.True(t, stats.Throughput > 0)
}

func TestMeteredChannel_Concurrent(t *testing.T) {
	ch := NewMeteredChannel[int]("test", 100)

	var wg sync.WaitGroup
	const numSenders = 5
	const numReceivers = 5
	const msgsPerSender = 100

	// Start senders
	for i := 0; i < numSenders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < msgsPerSender; j++ {
				ch.Send(id*1000 + j)
			}
		}(i)
	}

	// Start receivers
	received := make(chan int, numSenders*msgsPerSender)
	for i := 0; i < numReceivers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				val, ok := ch.TryReceive()
				if ok {
					received <- val
				} else {
					// Briefly wait if nothing to receive
					time.Sleep(time.Millisecond)
				}
				if len(received) >= numSenders*msgsPerSender {
					return
				}
			}
		}()
	}

	// Wait for all goroutines
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for concurrent operations")
	}

	stats := ch.Stats()
	assert.Equal(t, uint64(numSenders*msgsPerSender), stats.Sent)
}

func TestChannelMetrics_NewChannelMetrics(t *testing.T) {
	cm := NewChannelMetrics()
	require.NotNil(t, cm)
	assert.NotNil(t, cm.channels)
}

func TestChannelMetrics_Register(t *testing.T) {
	cm := NewChannelMetrics()

	ch1 := NewMeteredChannel[int]("channel1", 10)
	ch2 := NewMeteredChannel[string]("channel2", 20)

	cm.Register("ch1", ch1)
	cm.Register("ch2", ch2)

	stats := cm.AllStats()
	assert.Len(t, stats, 2)
	assert.Contains(t, stats, "ch1")
	assert.Contains(t, stats, "ch2")
}

func TestChannelMetrics_Unregister(t *testing.T) {
	cm := NewChannelMetrics()

	ch := NewMeteredChannel[int]("channel", 10)
	cm.Register("ch", ch)

	assert.Len(t, cm.AllStats(), 1)

	cm.Unregister("ch")

	assert.Len(t, cm.AllStats(), 0)
}

func TestChannelMetrics_AllStats(t *testing.T) {
	cm := NewChannelMetrics()

	ch1 := NewMeteredChannel[int]("channel1", 10)
	ch2 := NewMeteredChannel[int]("channel2", 20)

	ch1.Send(1)
	ch1.Send(2)
	ch2.Send(3)

	cm.Register("ch1", ch1)
	cm.Register("ch2", ch2)

	stats := cm.AllStats()

	assert.Equal(t, uint64(2), stats["ch1"].Sent)
	assert.Equal(t, uint64(1), stats["ch2"].Sent)
}

func TestChannelMetrics_TotalPending(t *testing.T) {
	cm := NewChannelMetrics()

	ch1 := NewMeteredChannel[int]("channel1", 10)
	ch2 := NewMeteredChannel[int]("channel2", 20)

	ch1.Send(1)
	ch1.Send(2)
	ch2.Send(3)
	ch2.Send(4)
	ch2.Send(5)

	cm.Register("ch1", ch1)
	cm.Register("ch2", ch2)

	assert.Equal(t, 5, cm.TotalPending())

	ch1.Receive()
	assert.Equal(t, 4, cm.TotalPending())
}

func TestChannelMetrics_TotalDropped(t *testing.T) {
	cm := NewChannelMetrics()

	ch1 := NewMeteredChannel[int]("channel1", 1)
	ch2 := NewMeteredChannel[int]("channel2", 1)

	// Fill channels
	ch1.Send(1)
	ch2.Send(1)

	// These should be dropped
	ch1.TrySend(2)
	ch1.TrySend(3)
	ch2.TrySend(2)

	cm.Register("ch1", ch1)
	cm.Register("ch2", ch2)

	assert.Equal(t, uint64(3), cm.TotalDropped())
}

func TestMeteredChannelStats_Fields(t *testing.T) {
	stats := MeteredChannelStats{
		Name:       "test",
		Sent:       100,
		Received:   80,
		Dropped:    5,
		Pending:    20,
		Capacity:   50,
		Throughput: 1000.5,
		Uptime:     time.Minute,
	}

	assert.Equal(t, "test", stats.Name)
	assert.Equal(t, uint64(100), stats.Sent)
	assert.Equal(t, uint64(80), stats.Received)
	assert.Equal(t, uint64(5), stats.Dropped)
	assert.Equal(t, 20, stats.Pending)
	assert.Equal(t, 50, stats.Capacity)
	assert.Equal(t, 1000.5, stats.Throughput)
	assert.Equal(t, time.Minute, stats.Uptime)
}

func TestMeteredChannel_ZeroThroughput(t *testing.T) {
	// Create a channel with minimal uptime
	ch := NewMeteredChannel[int]("test", 10)

	// Stats immediately after creation
	stats := ch.Stats()

	// Throughput should be 0 or very small since no messages sent
	assert.Equal(t, float64(0), stats.Throughput)
}

func TestMeteredChannel_StringType(t *testing.T) {
	ch := NewMeteredChannel[string]("string-channel", 5)

	ch.Send("hello")
	ch.Send("world")

	assert.Equal(t, "hello", ch.Receive())
	assert.Equal(t, "world", ch.Receive())
}

func TestMeteredChannel_StructType(t *testing.T) {
	type TestStruct struct {
		ID   int
		Name string
	}

	ch := NewMeteredChannel[TestStruct]("struct-channel", 5)

	ch.Send(TestStruct{ID: 1, Name: "one"})
	ch.Send(TestStruct{ID: 2, Name: "two"})

	val := ch.Receive()
	assert.Equal(t, 1, val.ID)
	assert.Equal(t, "one", val.Name)
}
