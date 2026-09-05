package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewClientIPResolverRejectsInvalidProxyCIDRs(t *testing.T) {
	t.Parallel()
	for _, cidr := range []string{"", "not-a-cidr", "192.0.2.1"} {
		t.Run(cidr, func(t *testing.T) {
			t.Parallel()
			if _, err := NewClientIPResolver(cidr); err == nil {
				t.Fatalf("NewClientIPResolver(%q) error = nil, want non-nil", cidr)
			}
		})
	}
}

func TestClientIPResolverUsesDirectPeerByDefault(t *testing.T) {
	t.Parallel()
	resolver, err := NewClientIPResolver()
	if err != nil {
		t.Fatalf("NewClientIPResolver() error = %v", err)
	}

	tests := []struct {
		name       string
		remoteAddr string
		forwarded  string
		want       string
	}{
		{name: "IPv4 with port", remoteAddr: "192.0.2.10:4321", want: "192.0.2.10"},
		{name: "IPv6 with port", remoteAddr: "[2001:db8::10]:4321", want: "2001:db8::10"},
		{name: "bare address", remoteAddr: "192.0.2.11", want: "192.0.2.11"},
		{name: "mapped IPv4", remoteAddr: "[::ffff:192.0.2.12]:4321", want: "192.0.2.12"},
		{
			name:       "unconfigured forwarding header is ignored",
			remoteAddr: "192.0.2.13:4321",
			forwarded:  "198.51.100.100",
			want:       "192.0.2.13",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.RemoteAddr = test.remoteAddr
			request.Header.Set("X-Forwarded-For", test.forwarded)
			got, err := resolver.Resolve(request)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if got.String() != test.want {
				t.Fatalf("Resolve() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestClientIPResolverTrustsForwardingChainOnlyFromConfiguredPeer(t *testing.T) {
	t.Parallel()
	resolver, err := NewClientIPResolver("10.0.0.0/8", "2001:db8:ffff::/48")
	if err != nil {
		t.Fatalf("NewClientIPResolver() error = %v", err)
	}

	tests := []struct {
		name       string
		remoteAddr string
		forwarded  []string
		want       string
	}{
		{
			name:       "untrusted peer cannot spoof client",
			remoteAddr: "192.0.2.10:4321",
			forwarded:  []string{"198.51.100.1"},
			want:       "192.0.2.10",
		},
		{
			name:       "trusted peer supplies client",
			remoteAddr: "10.0.0.3:4321",
			forwarded:  []string{"198.51.100.2"},
			want:       "198.51.100.2",
		},
		{
			name:       "trusted hops are removed from right",
			remoteAddr: "10.0.0.3:4321",
			forwarded:  []string{"203.0.113.7, 198.51.100.20, 10.0.0.2"},
			want:       "198.51.100.20",
		},
		{
			name:       "repeated headers form one chain",
			remoteAddr: "10.0.0.3:4321",
			forwarded:  []string{"203.0.113.8, 198.51.100.21", "10.0.0.2"},
			want:       "198.51.100.21",
		},
		{
			name:       "malformed chain safely falls back to peer",
			remoteAddr: "10.0.0.3:4321",
			forwarded:  []string{"not-an-ip, 10.0.0.2"},
			want:       "10.0.0.3",
		},
		{
			name:       "fully trusted chain safely falls back to peer",
			remoteAddr: "10.0.0.3:4321",
			forwarded:  []string{"10.0.0.1, 10.0.0.2"},
			want:       "10.0.0.3",
		},
		{
			name:       "trusted IPv6 peer",
			remoteAddr: "[2001:db8:ffff::10]:4321",
			forwarded:  []string{"2001:db8:1234::20"},
			want:       "2001:db8:1234::20",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.RemoteAddr = test.remoteAddr
			request.Header["X-Forwarded-For"] = test.forwarded
			got, err := resolver.Resolve(request)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if got.String() != test.want {
				t.Fatalf("Resolve() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestClientIPResolverRejectsInvalidInputs(t *testing.T) {
	t.Parallel()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.RemoteAddr = "invalid address"
	resolver, err := NewClientIPResolver()
	if err != nil {
		t.Fatalf("NewClientIPResolver() error = %v", err)
	}
	if _, err := resolver.Resolve(request); err == nil {
		t.Fatal("Resolve(invalid RemoteAddr) error = nil, want non-nil")
	}
	if _, err := resolver.Resolve(nil); err == nil {
		t.Fatal("Resolve(nil request) error = nil, want non-nil")
	}
	var nilResolver *ClientIPResolver
	if _, err := nilResolver.Resolve(httptest.NewRequest(http.MethodGet, "/", nil)); err == nil {
		t.Fatal("nil resolver Resolve() error = nil, want non-nil")
	}
}
