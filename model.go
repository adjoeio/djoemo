package djoemo

// Model ...
type Model struct {
	Version   uint
	CreatedAt *DjoemoTime
	UpdatedAt *DjoemoTime
}

// GetVersion returns the current version of the item from dynamo
func (m *Model) GetVersion() uint {
	return m.Version
}

// IncreaseVersion increases the current version by 1
func (m *Model) IncreaseVersion() {
	m.Version = m.Version + 1
}

// InitCreatedAt sets the CreatedAt field of the item if it hasnt been set
func (m *Model) InitCreatedAt() {
	if m.CreatedAt == nil {
		now := Now()
		m.CreatedAt = &now
	}
}

// InitUpdatedAt sets the UpdatedAt
func (m *Model) InitUpdatedAt() {
	now := Now()
	m.UpdatedAt = &now
}
