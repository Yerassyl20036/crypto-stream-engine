package config

import (
	"os"
	"strconv"
	"strings"
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
	prefix := os.Getenv("CONFIG_PREFIX")

	return &Config{
		HTTPAddr:                getEnv(prefix, "HTTP_ADDR", ":8080"),
		WSAddr:                  getEnv(prefix, "WS_ADDR", ":8080"),
		WorkerPoolSize:          getEnvInt(prefix, "WORKER_POOL_SIZE", 10),
		ChannelBufferSize:       getEnvInt(prefix, "CHANNEL_BUFFER_SIZE", 10000),
		VWAPWindowSeconds:       getEnvInt(prefix, "VWAP_WINDOW_SECONDS", 60),
		VolatilityWindowSeconds: getEnvInt(prefix, "VOLATILITY_WINDOW_SECONDS", 10),
		SpreadThresholdPercent:  getEnvFloat(prefix, "SPREAD_THRESHOLD_PERCENT", 0.5),
		VolatilityMultiplier:    getEnvFloat(prefix, "VOLATILITY_MULTIPLIER", 3.0),
		Symbols:                 getEnvSymbols(prefix, "SYMBOLS", []string{"BTCUSDT", "ETHUSDT"}),
		ShutdownTimeout:         getEnvDuration(prefix, "SHUTDOWN_TIMEOUT", 30*time.Second),
	}
}

func getEnv(prefix, key, defaultVal string) string {
	if val := lookupEnvWithPrefix(prefix, key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(prefix, key string, defaultVal int) int {
	if val := lookupEnvWithPrefix(prefix, key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat(prefix, key string, defaultVal float64) float64 {
	if val := lookupEnvWithPrefix(prefix, key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvDuration(prefix, key string, defaultVal time.Duration) time.Duration {
	if val := lookupEnvWithPrefix(prefix, key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}

func getEnvSymbols(prefix, key string, defaultVal []string) []string {
	val := lookupEnvWithPrefix(prefix, key)
	if val == "" {
		return defaultVal
	}

	parts := strings.Split(val, ",")
	symbols := make([]string, 0, len(parts))
	for _, part := range parts {
		symbol := strings.ToUpper(strings.TrimSpace(part))
		if symbol != "" {
			symbols = append(symbols, symbol)
		}
	}

	if len(symbols) == 0 {
		return defaultVal
	}

	return symbols
}

func lookupEnvWithPrefix(prefix, key string) string {
	if prefix != "" {
		if val := os.Getenv(prefix + key); val != "" {
			return val
		}
	}

	return os.Getenv(key)
}
