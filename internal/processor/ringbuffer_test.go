package processor

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func BenchmarkRingBufferAdd(b *testing.B) {
	rb := NewRingBuffer(60)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Add(100.0+rand.Float64(), rand.Float64()*10, time.Now())
	}
}

func BenchmarkRingBufferVWAP(b *testing.B) {
	rb := NewRingBuffer(60)

	// Populate buffer
	for i := 0; i < 60; i++ {
		rb.Add(100.0+rand.Float64(), rand.Float64()*10, time.Now())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rb.VWAP()
	}
}

func BenchmarkRingBufferStdDev(b *testing.B) {
	rb := NewRingBuffer(60)

	// Populate buffer
	for i := 0; i < 60; i++ {
		rb.Add(100.0+rand.Float64(), rand.Float64()*10, time.Now())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rb.StdDev()
	}
}

func BenchmarkRingBufferConcurrent(b *testing.B) {
	rb := NewRingBuffer(1000)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rb.Add(100.0+rand.Float64(), rand.Float64()*10, time.Now())
			_ = rb.VWAP()
		}
	})
}

func TestRingBufferVWAP(t *testing.T) {
	rb := NewRingBuffer(3)

	// Add test data: price=100, volume=1
	rb.Add(100, 1, time.Now())
	if vwap := rb.VWAP(); vwap != 100 {
		t.Errorf("Expected VWAP 100, got %f", vwap)
	}

	// Add: price=200, volume=1 -> VWAP should be 150
	rb.Add(200, 1, time.Now())
	if vwap := rb.VWAP(); vwap != 150 {
		t.Errorf("Expected VWAP 150, got %f", vwap)
	}

	// Add: price=300, volume=2 -> VWAP = (100*1 + 200*1 + 300*2) / (1+1+2) = 900/4 = 225
	rb.Add(300, 2, time.Now())
	if vwap := rb.VWAP(); vwap != 225 {
		t.Errorf("Expected VWAP 225, got %f", vwap)
	}

	// Add 4th element, should evict first (100, 1)
	// VWAP = (200*1 + 300*2 + 400*1) / (1+2+1) = 1200/4 = 300
	rb.Add(400, 1, time.Now())
	if vwap := rb.VWAP(); vwap != 300 {
		t.Errorf("Expected VWAP 300, got %f", vwap)
	}
}

func TestRingBufferThreadSafety(t *testing.T) {
	rb := NewRingBuffer(100)
	done := make(chan bool)

	// Multiple writers
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				rb.Add(rand.Float64()*100, rand.Float64()*10, time.Now())
			}
			done <- true
		}()
	}

	// Multiple readers
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				_ = rb.VWAP()
				_ = rb.StdDev()
				_ = rb.Len()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}
}

func TestRingBufferMeanAfterWrap(t *testing.T) {
	rb := NewRingBuffer(3)
	now := time.Now()

	rb.Add(1, 1, now)
	rb.Add(2, 1, now)
	rb.Add(3, 1, now)

	if got := rb.Mean(); got != 2 {
		t.Fatalf("expected mean 2, got %f", got)
	}

	rb.Add(10, 1, now) // evicts 1, active set should be [2,3,10]

	if got := rb.Mean(); got != 5 {
		t.Fatalf("expected mean 5 after wrap, got %f", got)
	}
}

func TestRingBufferStdDevAfterWrap(t *testing.T) {
	rb := NewRingBuffer(3)
	now := time.Now()

	rb.Add(1, 1, now)
	rb.Add(2, 1, now)
	rb.Add(3, 1, now)
	rb.Add(10, 1, now) // active set: [2,3,10]

	variance := rb.StdDev()
	if math.Abs(variance-12.6666666667) > 1e-9 {
		t.Fatalf("expected variance ~12.6666666667, got %f", variance)
	}
}
