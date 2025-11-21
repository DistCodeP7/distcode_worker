package worker

import (
	"archive/tar"
	"bytes"
	"io"

	t "github.com/DistCodeP7/distcode_worker/types"
)

func createTarStream(files t.FileMap) (io.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for name, content := range files {
		hdr := &tar.Header{
			Name: string(name),
			Mode: 0o644,
			Size: int64(len(content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}

	return &buf, nil
}
