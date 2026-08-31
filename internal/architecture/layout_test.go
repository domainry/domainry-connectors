package architecture

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestLayerDependenciesPointInward(t *testing.T) {
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	rules := map[string][]string{
		"internal/domain":      {"internal/application", "internal/adapter", "internal/assembly", "internal/infrastructure"},
		"internal/application": {"internal/adapter", "internal/assembly", "internal/infrastructure"},
		"internal/adapter":     {"internal/assembly", "internal/infrastructure"},
	}
	for relativeRoot, forbidden := range rules {
		root := filepath.Join(repositoryRoot, filepath.FromSlash(relativeRoot))
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if parseErr != nil {
				return parseErr
			}
			for _, specification := range file.Imports {
				importPath, unquoteErr := strconv.Unquote(specification.Path.Value)
				if unquoteErr != nil {
					return unquoteErr
				}
				for _, segment := range forbidden {
					if strings.Contains(importPath, "domainry-connectors/"+segment) {
						t.Errorf("%s imports outward layer %s", path, importPath)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
