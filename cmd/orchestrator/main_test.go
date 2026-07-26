package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
)

func TestAPIServerStartErrorPortInUseHint(t *testing.T) {
	err := errors.New("listen tcp :8091: bind: Only one usage of each socket address (protocol/network address/port) is normally permitted")
	msg := apiServerStartError(":8091", err)
	if !strings.Contains(msg, "already in use") {
		t.Fatalf("expected already-in-use hint, got %q", msg)
	}
	if !strings.Contains(msg, "-api-addr") {
		t.Fatalf("expected -api-addr hint, got %q", msg)
	}
}

func TestAPIServerStartErrorPermissionHint(t *testing.T) {
	err := errors.New("listen tcp :80: bind: permission denied")
	msg := apiServerStartError(":80", err)
	if !strings.Contains(msg, "insufficient permission") {
		t.Fatalf("expected permission hint, got %q", msg)
	}
	if !strings.Contains(msg, "non-privileged port") {
		t.Fatalf("expected non-privileged port hint, got %q", msg)
	}
}

func TestAPIServerStartErrorFallback(t *testing.T) {
	err := errors.New("context canceled")
	msg := apiServerStartError(":8080", err)
	if !strings.Contains(msg, "api server error: context canceled") {
		t.Fatalf("unexpected fallback message: %q", msg)
	}
}

func TestCheckAPIAddrAvailableDetectsConflict(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	err = checkAPIAddrAvailable(listener.Addr().String())
	if err == nil {
		t.Fatalf("expected preflight conflict error")
	}
	msg := apiServerStartError(listener.Addr().String(), err)
	if !strings.Contains(strings.ToLower(msg), "already in use") {
		t.Fatalf("expected occupied-address hint, got %q", msg)
	}
}

func TestResolveAPIAddrWithCheckNoFallbackWhenAvailable(t *testing.T) {
	check := func(addr string) error {
		if addr == ":8091" {
			return nil
		}
		return fmt.Errorf("unexpected addr: %s", addr)
	}

	resolved, err := resolveAPIAddrWithCheck(":8091", true, 5, check)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved != ":8091" {
		t.Fatalf("expected :8091, got %s", resolved)
	}
}

func TestResolveAPIAddrWithCheckFallbackEnabled(t *testing.T) {
	check := func(addr string) error {
		if addr == ":8091" || addr == ":8092" {
			return errors.New("in use")
		}
		if addr == ":8093" {
			return nil
		}
		return errors.New("unexpected")
	}

	resolved, err := resolveAPIAddrWithCheck(":8091", true, 5, check)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved != ":8093" {
		t.Fatalf("expected :8093, got %s", resolved)
	}
}

func TestResolveAPIAddrWithCheckFallbackDisabled(t *testing.T) {
	check := func(addr string) error {
		return errors.New("in use")
	}

	_, err := resolveAPIAddrWithCheck(":8091", false, 5, check)
	if err == nil {
		t.Fatalf("expected error when fallback disabled")
	}
}
