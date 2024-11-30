package pubsub

import "reflect"

func fullyQualifiedName(typ reflect.Type) string {
	return typ.PkgPath() + "." + typ.Name()
}
