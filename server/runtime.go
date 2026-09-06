package server

import (
	"reflect"

	"github.com/MontFerret/api"
)

func isNilRuntime(runtime api.Runtime) bool {
	if runtime == nil {
		return true
	}

	value := reflect.ValueOf(runtime)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
