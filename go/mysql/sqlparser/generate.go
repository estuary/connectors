/*
Copyright 2024 Estuary Technologies, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

// The parser is generated from sql.y by goyacc. Unlike upstream Vitess, which
// carries a bundled goyacc fork plus three AST-helper generators (ASTHelperGen,
// ASTFmtGen, Sizegen), this package uses the standard golang.org/x/tools goyacc
// and hand-maintains the AST. To regenerate after editing the grammar:
//
//	go generate ./go/mysql/sqlparser/
//
//go:generate go run golang.org/x/tools/cmd/goyacc -o sql.go sql.y
