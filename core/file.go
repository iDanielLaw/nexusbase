package core

type FileRemover interface {
	Remove(name string) error
}
