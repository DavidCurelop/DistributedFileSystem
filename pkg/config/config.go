// Package config holds helpers shared by the DFS binaries to resolve
// settings from environment variables and normalize network addresses.
package config

import (
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

// EnvString returns the value of the environment variable key, or def when it is unset or empty.
func EnvString(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

// EnvInt returns the integer value of the environment variable key, or def
// when it is unset or not a valid integer.
func EnvInt(key string, def int) int {
	value := os.Getenv(key)
	if value == "" {
		return def
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("ignoring invalid %s=%q: %v", key, value, err)
		return def
	}
	return parsed
}

// ListenAddr turns a bare port ("50051") into a listen address (":50051");
// values that already contain a colon are returned unchanged.
func ListenAddr(port string) string {
	if strings.Contains(port, ":") {
		return port
	}
	return ":" + port
}

// WithDefaultPort appends port to addr when addr has no port of its own.
func WithDefaultPort(addr, port string) string {
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	return net.JoinHostPort(addr, port)
}

// Port returns the port part of a listen address such as ":50052".
func Port(listenAddr string) string {
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return strings.TrimPrefix(listenAddr, ":")
	}
	return port
}