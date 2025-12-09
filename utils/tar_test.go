package utils

import (
	"archive/tar"
	"io"
	"testing"

	"github.com/DistCodeP7/distcode_worker/types"
)

func TestCreateTarStream(t *testing.T) {
	files := types.FileMap{
		"file1.txt": "hello",
		"file2.txt": "world",
	}

	r, err := CreateTarStream(files)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tr := tar.NewReader(r)

	seen := map[string]string{}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to read tar entry: %v", err)
		}

		content, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("failed to read file content: %v", err)
		}

		seen[hdr.Name] = string(content)
	}

	// Verify all files are present and correct
	for name, content := range files {
		got, ok := seen[string(name)]
		if !ok {
			t.Errorf("missing file in tar: %s", name)
		}
		if got != string(content) {
			t.Errorf("content mismatch for %s: got %q, want %q", name, got, content)
		}
	}
}
