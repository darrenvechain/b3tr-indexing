package indexer

const (
	Initialised = iota
	Syncing
	Synced
	Error
)

func Status(status int) string {
	switch status {
	case Initialised:
		return "Initialised"
	case Syncing:
		return "Syncing"
	case Synced:
		return "Synced"
	case Error:
		return "Error"
	default:
		return "Unknown"
	}
}
