package storage

import "kpi-dashboard-backend/model"

type InMemoryStore struct {
	kpis []model.KPIEvent
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		kpis: []model.KPIEvent{},
	}
}

func (s *InMemoryStore) Add(event model.KPIEvent) {
	s.kpis = append(s.kpis, event)
}

func (s *InMemoryStore) GetAll() []model.KPIEvent {
	return s.kpis
}
