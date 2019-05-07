// DO NOT EDIT: This file is autogenerated via the builtin command.

package stdlib

import (
	ast "github.com/influxdata/flux/ast"
	fluxtests "github.com/influxdata/flux/stdlib/influxdata/influxdb/v1/fluxtests"
	testdata "github.com/influxdata/flux/stdlib/testing/testdata"
)

var FluxTestPackages = func() []*ast.Package {
	var pkgs []*ast.Package
	pkgs = append(pkgs, fluxtests.FluxTestPackages...)
	pkgs = append(pkgs, testdata.FluxTestPackages...)
	return pkgs
}()
