package worker

import (
	"time"
)

type Clock interface {
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) Ticker
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type RealClock struct{}

func (RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (RealClock) NewTicker(d time.Duration) Ticker {
	return &RealTicker{time.NewTicker(d)}
}

type RealTicker struct {
	*time.Ticker
}

func (rt *RealTicker) C() <-chan time.Time {
	return rt.Ticker.C
}
