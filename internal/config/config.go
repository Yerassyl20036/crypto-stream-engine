package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	// Server configuration
	HTTPAddr string
	WSAddr   string

	// Pipeline configuration
	WorkerPoolSize    int
	ChannelBufferSize int

	// Processing windows
	VWAPWindowSeconds       int
	VolatilityWindowSeconds int

	// Alert thresholds
	SpreadThresholdPercent float64
	VolatilityMultiplier   float64

	// Symbols to track
	Symbols []string

	// Graceful shutdown timeout
	ShutdownTimeout time.Duration
}

// Load reads configuration from environment variables with sensible defaults
func Load() *Config {
	return &Config{
		HTTPAddr:                getEnv("HTTP_ADDR", ":8080"),
		WSAddr:                  getEnv("WS_ADDR", ":8080"),
		WorkerPoolSize:          getEnvInt("WORKER_POOL_SIZE", 10),
		ChannelBufferSize:       getEnvInt("CHANNEL_BUFFER_SIZE", 10000),
		VWAPWindowSeconds:       getEnvInt("VWAP_WINDOW_SECONDS", 60),
		VolatilityWindowSeconds: getEnvInt("VOLATILITY_WINDOW_SECONDS", 10),
		SpreadThresholdPercent:  getEnvFloat("SPREAD_THRESHOLD_PERCENT", 0.5),
		VolatilityMultiplier:    getEnvFloat("VOLATILITY_MULTIPLIER", 3.0),
		Symbols:                 []string{"BTCUSDT", "ETHUSDT"},
		ShutdownTimeout:         getEnvDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
