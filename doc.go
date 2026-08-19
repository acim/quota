// Package quota provides fixed-window admission control for arbitrary units.
//
// A unit can represent a request, byte, job, recipient, model token, or any
// other non-negative quantity chosen by the caller. Counters are isolated by
// namespace and bucket and reset at UTC-aligned window boundaries.
package quota
