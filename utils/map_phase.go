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

func MapToPayload(timeSpent map[types.Phase]time.Duration) TimeSpentPayload {
	return TimeSpentPayload{
		Compiling:          int64(timeSpent[types.PhaseCompiling].Milliseconds()),
		Running:            int64(timeSpent[types.PhaseRunning].Milliseconds()),
		Reserving:          int64(timeSpent[types.PhaseReserving].Milliseconds()),
		Pending:            int64(timeSpent[types.PhasePending].Milliseconds()),
		ConfiguringNetwork: int64(timeSpent[types.PhaseConfiguringNetwork].Milliseconds()),
	}
}
