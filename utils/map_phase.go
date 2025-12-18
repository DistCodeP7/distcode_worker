package utils

import (
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
)

type TimeSpentPayload struct {
	Compiling          int64 `json:"time_compiling"`
	Running            int64 `json:"time_running"`
	Reserving          int64 `json:"time_reserving"`
	Pending            int64 `json:"time_pending"`
	ConfiguringNetwork int64 `json:"time_configuring_network"`
}

// HasPhases checks if all the specified phases have a duration > 0.
func (ts *TimeSpentPayload) HasPhases(required ...types.Phase) bool {
	for _, phase := range required {
		switch phase {
		case types.PhaseCompiling:
			if ts.Compiling == 0 {
				return false
			}
		case types.PhaseRunning:
			if ts.Running == 0 {
				return false
			}
		case types.PhaseReserving:
			if ts.Reserving == 0 {
				return false
			}
		case types.PhasePending:
			if ts.Pending == 0 {
				return false
			}
		case types.PhaseConfiguringNetwork:
			if ts.ConfiguringNetwork == 0 {
				return false
			}
		}
	}
	return true
}

func MapToPayload(timeSpent map[types.Phase]time.Duration) TimeSpentPayload {
	return TimeSpentPayload{
		Compiling:          timeSpent[types.PhaseCompiling].Milliseconds(),
		Running:            timeSpent[types.PhaseRunning].Milliseconds(),
		Reserving:          timeSpent[types.PhaseReserving].Milliseconds(),
		Pending:            timeSpent[types.PhasePending].Milliseconds(),
		ConfiguringNetwork: timeSpent[types.PhaseConfiguringNetwork].Milliseconds(),
	}
}
