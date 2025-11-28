package types

// E.g exercises/mutex/example.go or main.go
type Path string
type SourceCode string
type FileMap map[Path]SourceCode
type EnvironmentVariable struct {
	Key   string
	Value string
}

type NodeSpec struct {
	Files FileMap
	Envs  []EnvironmentVariable
	Alias string

	// Could be "go build -o binary /tmp/hello/main.go /tmp/hello/utility.go"
	BuildCommand string

	// Must point to the executable built by BuildCommand, e.g. "/tmp/hello/binary"
	EntryCommand string
}
