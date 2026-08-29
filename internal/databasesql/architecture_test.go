package databasesql

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestExternalDatabaseProvidersUseRuntimeOwnedSQLTransport(t *testing.T) {
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	roots := []string{
		filepath.Join(repositoryRoot, "providers", "external_database"),
		filepath.Join(repositoryRoot, "internal", "snowflakesql"),
	}
	for _, root := range roots {
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
			for _, importSpec := range file.Imports {
				importPath, unquoteErr := strconv.Unquote(importSpec.Path.Value)
				if unquoteErr != nil {
					return unquoteErr
				}
				if forbiddenExternalDatabaseImport(importPath) {
					relative, _ := filepath.Rel(repositoryRoot, path)
					t.Errorf("%s imports %q; external database providers must use connector.Transport.ExecuteSQL", relative, importPath)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func forbiddenExternalDatabaseImport(importPath string) bool {
	if importPath == "database/sql" || strings.HasPrefix(importPath, "github.com/domainry/domainry-orm") {
		return true
	}
	for _, driver := range []string{
		"github.com/go-sql-driver/mysql",
		"github.com/jackc/pgx",
		"github.com/microsoft/go-mssqldb",
		"github.com/snowflakedb/gosnowflake",
	} {
		if importPath == driver || strings.HasPrefix(importPath, driver+"/") {
			return true
		}
	}
	return false
}
