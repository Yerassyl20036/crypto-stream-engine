package processor

import (
	"sync"
	"time"
)

// PricePoint represents a single data point in the ring buffer
type PricePoint struct {
	Price     float64
	Volume    float64
	Timestamp time.Time
}

// RingBuffer implements a fixed-size circular buffer for O(1) operations
// Thread-safe for concurrent reads/writes
type RingBuffer struct {
	mu     sync.RWMutex
	buffer []PricePoint
	size   int
	head   int  // Next write position
	full   bool // Whether we've wrapped around

	// Cached aggregates for O(1) access
	weightedPriceSum float64
	priceSum         float64
	priceSquaresSum  float64
	volumeSum        float64
	count            int
}

// NewRingBuffer creates a ring buffer with the given capacity
func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		buffer: make([]PricePoint, size),
		size:   size,
	}
}

// Add inserts a new price point, evicting the oldest if full
// Returns the evicted point (if any) for updating aggregates
func (rb *RingBuffer) Add(price, volume float64, ts time.Time) (evicted *PricePoint) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// If buffer is full, we'll overwrite the oldest point
	if rb.full {
		evicted = &PricePoint{
			Price:     rb.buffer[rb.head].Price,
			Volume:    rb.buffer[rb.head].Volume,
			Timestamp: rb.buffer[rb.head].Timestamp,
		}

		// Update cached sums by removing old value
		rb.weightedPriceSum -= evicted.Price * evicted.Volume
		rb.priceSum -= evicted.Price
		rb.priceSquaresSum -= evicted.Price * evicted.Price
		rb.volumeSum -= evicted.Volume
	} else {
		rb.count++
	}

	// Add new point
	rb.buffer[rb.head] = PricePoint{
		Price:     price,
		Volume:    volume,
		Timestamp: ts,
	}

	// Update cached sums
	rb.weightedPriceSum += price * volume
	rb.priceSum += price
	rb.priceSquaresSum += price * price
	rb.volumeSum += volume

	// Advance head
	rb.head = (rb.head + 1) % rb.size
	if rb.head == 0 {
		rb.full = true
	}

	return evicted
}

// VWAP calculates Volume-Weighted Average Price in O(1)
func (rb *RingBuffer) VWAP() float64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.volumeSum == 0 {
		return 0
	}
	return rb.weightedPriceSum / rb.volumeSum
}

// Mean calculates simple average price in O(1)
func (rb *RingBuffer) Mean() float64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return 0
	}

	return rb.priceSum / float64(rb.count)
}

// StdDev calculates standard deviation for volatility detection
// This is O(n) but n is small (e.g., 60 for 1-minute window)
func (rb *RingBuffer) StdDev() float64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count < 2 {
		return 0
	}

	mean := rb.priceSum / float64(rb.count)
	variance := (rb.priceSquaresSum / float64(rb.count)) - (mean * mean)
	if variance < 0 {
		return 0
	}
	return variance // Return variance; caller can sqrt() if needed
}

// Len returns the current number of elements
func (rb *RingBuffer) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.count
}

// IsFull returns whether the buffer has reached capacity
func (rb *RingBuffer) IsFull() bool {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.full
}

// GetRecent returns the n most recent points (or all if n > count)
func (rb *RingBuffer) GetRecent(n int) []PricePoint {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if n > rb.count {
		n = rb.count
	}

	result := make([]PricePoint, n)
	for i := 0; i < n; i++ {
		idx := (rb.head - 1 - i + rb.size) % rb.size
		result[i] = rb.buffer[idx]
	}

	return result
}
