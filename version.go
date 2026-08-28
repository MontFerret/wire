package wire

import "runtime/debug"

const (
	apiIdentity = "ferret.wire.v1"

	wireModulePath = "github.com/MontFerret/wire"
)

func moduleVersion(path, fallback string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return fallback
	}

	if info.Main.Path == path && usableVersion(info.Main.Version) {
		return info.Main.Version
	}

	for _, dependency := range info.Deps {
		if dependency.Path != path {
			continue
		}

		if usableVersion(dependency.Version) {
			return dependency.Version
		}

		if dependency.Replace != nil && usableVersion(dependency.Replace.Version) {
			return dependency.Replace.Version
		}
	}

	return fallback
}

func usableVersion(value string) bool {
	return value != "" && value != "(devel)"
}
