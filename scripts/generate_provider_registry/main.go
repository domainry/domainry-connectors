// Command generate_provider_registry generates the explicit, statically linked
// Provider constructor inventory consumed only by the official Catalog tool.
// It does not generate Runtime composition and never relies on init hooks.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	modulePath = "github.com/domainry/domainry-connectors/providers"
	outputPath = "scripts/generate_catalog/provider_registry_generated.go"
)

var stableKey = regexp.MustCompile(`^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$`)

var forbiddenGenericKey = map[string]bool{
	"common": true, "library": true, "shared": true, "util": true, "utils": true,
}

type provider struct {
	ImportPath  string
	PackageName string
	Alias       string
}

func main() {
	check := flag.Bool("check", false, "fail when the generated Provider registry is stale")
	flag.Parse()
	root, err := repositoryRoot()
	if err != nil {
		fatal(err)
	}
	providers, err := discover(filepath.Join(root, "providers"))
	if err != nil {
		fatal(err)
	}
	content, err := render(providers)
	if err != nil {
		fatal(err)
	}
	destination := filepath.Join(root, outputPath)
	if *check {
		existing, err := os.ReadFile(destination)
		if err != nil || !bytes.Equal(existing, content) {
			fatal(fmt.Errorf("official Provider registry is stale; run go run ./scripts/generate_provider_registry"))
		}
		return
	}
	if err := os.WriteFile(destination, content, 0o644); err != nil {
		fatal(err)
	}
}

func repositoryRoot() (string, error) {
	current, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("go.mod not found")
		}
		current = parent
	}
}

func discover(root string) ([]provider, error) {
	providers := []provider{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() || path == root {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(relative), "/")
		if len(parts) < 2 {
			return nil
		}
		if len(parts) > 2 {
			return filepath.SkipDir
		}
		if !stableKey.MatchString(parts[0]) || !stableKey.MatchString(parts[1]) || forbiddenGenericKey[parts[0]] || forbiddenGenericKey[parts[1]] {
			return fmt.Errorf("Provider path %s must use two specific stable lower_snake_case keys", relative)
		}
		if err := rejectNestedGoPackages(path); err != nil {
			return fmt.Errorf("Provider %s: %w", relative, err)
		}
		packages, err := parser.ParseDir(token.NewFileSet(), path, func(info os.FileInfo) bool {
			return strings.HasSuffix(info.Name(), ".go") && !strings.HasSuffix(info.Name(), "_test.go")
		}, 0)
		if err != nil {
			return err
		}
		if len(packages) != 1 {
			return fmt.Errorf("Provider %s must contain exactly one production Go package", relative)
		}
		var packageName string
		constructorCount := 0
		for name, parsed := range packages {
			packageName = name
			for _, file := range parsed.Files {
				if hasConstructor(file) {
					constructorCount++
				}
			}
		}
		if packageName != strings.ReplaceAll(parts[1], "_", "") {
			return fmt.Errorf("Provider %s package name %s must be the idiomatic provider key", relative, packageName)
		}
		if constructorCount != 1 {
			return fmt.Errorf("Provider %s does not publish func New(connector.Transport) (connector.Adapter, error)", relative)
		}
		alias := sanitize(parts[0] + "_" + parts[1])
		providers = append(providers, provider{ImportPath: modulePath + "/" + filepath.ToSlash(relative), PackageName: packageName, Alias: alias})
		return filepath.SkipDir
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(providers, func(i, j int) bool { return providers[i].ImportPath < providers[j].ImportPath })
	return providers, nil
}

func rejectNestedGoPackages(providerRoot string) error {
	return filepath.WalkDir(providerRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == providerRoot || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		if filepath.Dir(path) != providerRoot {
			return fmt.Errorf("nested Go package is forbidden: %s", path)
		}
		return nil
	})
}

func hasConstructor(file *ast.File) bool {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv == nil && function.Name.Name == "New" && fieldCount(function.Type.Params) == 1 && fieldCount(function.Type.Results) == 2 {
			return true
		}
	}
	return false
}

func fieldCount(fields *ast.FieldList) int {
	if fields == nil {
		return 0
	}
	count := 0
	for _, field := range fields.List {
		if len(field.Names) == 0 {
			count++
		} else {
			count += len(field.Names)
		}
	}
	return count
}

func render(providers []provider) ([]byte, error) {
	var source strings.Builder
	source.WriteString("// Code generated by go run ./scripts/generate_provider_registry; DO NOT EDIT.\n\npackage main\n\nimport (\n")
	source.WriteString("\tconnector \"github.com/domainry/domainry-connector-sdk\"\n")
	for _, item := range providers {
		fmt.Fprintf(&source, "\t%s %q\n", item.Alias, item.ImportPath)
	}
	source.WriteString(")\n\nfunc providerSpecifications() []providerSpec {\n\treturn []providerSpec{\n")
	for _, item := range providers {
		fmt.Fprintf(&source, "\t\t{importPath: %q, packageName: %q, constructor: providerConstructor(%s.New)},\n", item.ImportPath, item.PackageName, item.Alias)
	}
	source.WriteString("\t}\n}\n\nfunc providerConstructor(constructor func(connector.Transport) (connector.Adapter, error)) func(connector.Transport) (connector.Adapter, error) {\n\treturn constructor\n}\n")
	formatted, err := format.Source([]byte(source.String()))
	if err != nil {
		return nil, fmt.Errorf("format generated Provider registry: %w", err)
	}
	return formatted, nil
}

func sanitize(value string) string {
	return strings.NewReplacer("-", "_", ".", "_").Replace(value)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
