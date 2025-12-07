package health

import "sync"

type HealthService interface {
	ServiceName() string
	Ok() (bool, string)
}

type HealthServiceRegister struct {
	services []HealthService
	mu       sync.RWMutex
}

func NewHealthServiceRegister() *HealthServiceRegister {
	return &HealthServiceRegister{
		services: []HealthService{},
	}
}

func (hr *HealthServiceRegister) Register(s ...HealthService) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.services = append(hr.services, s...)
}

func (hr *HealthServiceRegister) CheckAll() map[string]map[string]any {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	results := make(map[string]map[string]any)
	for _, service := range hr.services {
		ok, msg := service.Ok()
		results[service.ServiceName()] = map[string]any{
			"healthy": ok,
			"message": msg,
		}
	}
	return results
}

func (hr *HealthServiceRegister) OverallStatus() bool {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	for _, service := range hr.services {
		ok, _ := service.Ok()
		if !ok {
			return false
		}
	}
	return true
}
