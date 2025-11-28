package engine2

import (
	"expvar"
	"fmt"
)

// publishExpvarInt safely publishes an expvar.Int.
func publishExpvarInt(name string) *expvar.Int {
	v := expvar.Get(name)
	if v == nil {
		return expvar.NewInt(name)
	}
	if iv, ok := v.(*expvar.Int); ok {
		iv.Set(0)
		return iv
	}
	panic(fmt.Sprintf("expvar: trying to publish Int %s but variable already exists with different type %T", name, v))
}

// publishExpvarFloat safely publishes an expvar.Float.
func publishExpvarFloat(name string) *expvar.Float {
	v := expvar.Get(name)
	if v == nil {
		return expvar.NewFloat(name)
	}
	if fv, ok := v.(*expvar.Float); ok {
		fv.Set(0.0)
		return fv
	}
	panic(fmt.Sprintf("expvar: trying to publish Float %s but variable already exists with different type %T", name, v))
}

// publishExpvarMap safely publishes an expvar.Map.
func publishExpvarMap(name string) *expvar.Map {
	v := expvar.Get(name)
	if v == nil {
		return expvar.NewMap(name)
	}
	if mv, ok := v.(*expvar.Map); ok {
		return mv
	}
	panic(fmt.Sprintf("expvar: trying to publish Map %s but variable already exists with different type %T", name, v))
}
