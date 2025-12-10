package utils

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"

	t "github.com/DistCodeP7/distcode_worker/types"
)

func CreateTarStream(files t.FileMap) (io.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	var writeErr error
	defer func() {
		if err := tw.Close(); err != nil {
			writeErr = errors.Join(writeErr, err)
		}
	}()

	for name, content := range files {
		hdr := &tar.Header{
			Name: string(name),
			Mode: 0o644, // File permissions see https://www.redhat.com/en/blog/linux-file-permissions-explained
			Size: int64(len(content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			writeErr = errors.Join(writeErr, err)
			return nil, writeErr
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			writeErr = errors.Join(writeErr, err)
			return nil, writeErr
		}
	}

	return &buf, writeErr
}
