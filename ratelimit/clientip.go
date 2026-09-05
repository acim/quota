package ratelimit

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
)

// ClientIPResolver resolves the direct client or the first untrusted address
// in a forwarding chain. Forwarded addresses are considered only when the
// request's immediate peer belongs to a configured trusted proxy prefix.
type ClientIPResolver struct {
	trustedProxyPrefixes []netip.Prefix
}

// NewClientIPResolver constructs a resolver. Each trustedProxyCIDR must be a
// valid IPv4 or IPv6 prefix. With no trusted proxy CIDRs, Resolve always ignores
// forwarding headers and returns the normalized address from Request.RemoteAddr.
func NewClientIPResolver(trustedProxyCIDRs ...string) (*ClientIPResolver, error) {
	resolver := &ClientIPResolver{}
	for _, value := range trustedProxyCIDRs {
		value = strings.TrimSpace(value)
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return nil, fmt.Errorf("parse trusted proxy CIDR %q: %w", value, err)
		}
		resolver.trustedProxyPrefixes = append(resolver.trustedProxyPrefixes, normalizePrefix(prefix))
	}
	return resolver, nil
}

// Resolve returns the normalized client IP for request. It ignores
// X-Forwarded-For unless the immediate peer is trusted. For a trusted peer, it
// walks the forwarding chain from right to left and returns the first untrusted
// address. A malformed or fully trusted chain safely falls back to the peer.
func (resolver *ClientIPResolver) Resolve(request *http.Request) (netip.Addr, error) {
	if resolver == nil {
		return netip.Addr{}, errors.New("client IP resolver is required")
	}
	if request == nil {
		return netip.Addr{}, errors.New("HTTP request is required")
	}

	peer, err := parseRemoteAddr(request.RemoteAddr)
	if err != nil {
		return netip.Addr{}, err
	}
	if !containsIP(resolver.trustedProxyPrefixes, peer) {
		return peer, nil
	}

	forwarded := request.Header.Values("X-Forwarded-For")
	if len(forwarded) == 0 {
		return peer, nil
	}
	chain := strings.Split(strings.Join(forwarded, ","), ",")
	for _, value := range slices.Backward(chain) {
		address, err := netip.ParseAddr(strings.TrimSpace(value))
		if err != nil {
			return peer, nil
		}
		address = address.Unmap()
		if !containsIP(resolver.trustedProxyPrefixes, address) {
			return address, nil
		}
	}
	return peer, nil
}

func parseRemoteAddr(remoteAddr string) (netip.Addr, error) {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	address, err := netip.ParseAddr(strings.TrimSpace(host))
	if err != nil {
		return netip.Addr{}, fmt.Errorf("parse remote address %q: %w", remoteAddr, err)
	}
	return address.Unmap(), nil
}

func containsIP(prefixes []netip.Prefix, address netip.Addr) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

func normalizePrefix(prefix netip.Prefix) netip.Prefix {
	if prefix.Addr().Is4In6() && prefix.Bits() >= 96 {
		return netip.PrefixFrom(prefix.Addr().Unmap(), prefix.Bits()-96).Masked()
	}
	return prefix.Masked()
}
