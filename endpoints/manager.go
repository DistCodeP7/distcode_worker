package endpoints

import (
	"encoding/json"
	"sync"
)

type JSONCollector interface {
	JSON() []byte
	PartName() string
}

type MetricsManager struct {
	mu         sync.Mutex
	collectors []JSONCollector
}

func NewManager() *MetricsManager {
	return &MetricsManager{
		collectors: []JSONCollector{},
	}
}

type Manager interface {
	Register(c ...JSONCollector) *MetricsManager
	AggregateJSON() []byte
}

func (m *MetricsManager) Register(c ...JSONCollector) *MetricsManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.collectors = append(m.collectors, c...)
	return m
}

func (m *MetricsManager) AggregateJSON() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	aggregated := make(map[string]interface{})

	for _, c := range m.collectors {
		var data map[string]interface{}
		err := json.Unmarshal(c.JSON(), &data)
		if err != nil {
			continue
		}
		aggregated[c.PartName()] = data
	}

	b, _ := json.MarshalIndent(aggregated, "", "  ")
	return b
}
