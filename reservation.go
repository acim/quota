package quota

import (
	"context"
	"fmt"
	"sync"
)

type reservationState uint8

const (
	reservationOpen reservationState = iota
	reservationCommitted
	reservationReleased
)

// Reservation represents an amount admitted before its actual cost was known.
// Its finalization methods are safe for concurrent use and idempotent when
// repeated with the same result.
type Reservation struct {
	mu          sync.Mutex
	store       Store
	key         string
	reserved    int64
	state       reservationState
	finalAmount int64
}

// Reserved reports the upper bound admitted for the reservation.
func (r *Reservation) Reserved() int64 {
	return r.reserved
}

// Commit retains actual and refunds any unused part of the reservation.
func (r *Reservation) Commit(ctx context.Context, actual int64) error {
	if actual < 0 || actual > r.reserved {
		return fmt.Errorf("%w: actual amount must be between zero and %d", ErrInvalidRequest, r.reserved)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == reservationCommitted {
		if r.finalAmount == actual {
			return nil
		}
		return ErrReservationFinalized
	}
	if r.state == reservationReleased {
		if actual == 0 {
			return nil
		}
		return ErrReservationFinalized
	}

	if refund := r.reserved - actual; refund > 0 {
		if _, err := r.store.Refund(ctx, r.key, refund); err != nil {
			return fmt.Errorf("refund unused quota: %w", err)
		}
	}
	r.state = reservationCommitted
	r.finalAmount = actual
	return nil
}

// Release refunds the entire reservation.
func (r *Reservation) Release(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == reservationReleased || (r.state == reservationCommitted && r.finalAmount == 0) {
		return nil
	}
	if r.state == reservationCommitted {
		return ErrReservationFinalized
	}

	if _, err := r.store.Refund(ctx, r.key, r.reserved); err != nil {
		return fmt.Errorf("release quota: %w", err)
	}
	r.state = reservationReleased
	r.finalAmount = 0
	return nil
}
