package architecture

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAssemblyContainsOnlyDeploymentModes(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join("..", "assembly"))
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"module": true, "saas": true}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		if !entry.IsDir() || !allowed[entry.Name()] {
			t.Errorf("internal/assembly contains non-deployment entry %q", entry.Name())
		}
	}
}
