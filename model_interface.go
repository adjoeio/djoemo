package djoemo

// ModelInterface ...
type ModelInterface interface {
	GetVersion() uint
	IncreaseVersion()
	InitCreatedAt()
	InitUpdatedAt()
}
