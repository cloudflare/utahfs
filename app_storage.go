package utahfs

type AppStorage interface {
	Start()

	State() (map[string]interface{}, error)

	ObjectStorage

	Commit() error
	Rollback() error
}
