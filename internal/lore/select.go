package lore

import (
	"fmt"
	"sort"
	"strings"
)

func OrderedPaths(manifest Manifest) []string {
	paths := make([]string, 0, len(manifest))
	for path := range manifest {
		paths = append(paths, path)
	}
	sort.Slice(paths, func(i, j int) bool {
		if manifest[paths[i]].Modified != manifest[paths[j]].Modified {
			return manifest[paths[i]].Modified > manifest[paths[j]].Modified
		}
		return paths[i] < paths[j]
	})
	return paths
}

func SelectManifest(manifest Manifest, lists string) (Manifest, error) {
	selected := map[string]bool{}
	if lists != "" {
		for _, list := range strings.Split(lists, ",") {
			list = strings.TrimSpace(list)
			if _, err := RepositoryName("/" + list); err != nil {
				return nil, fmt.Errorf("invalid inbox %q: %w", list, err)
			}
			selected[list] = false
		}
	}
	result := Manifest{}
	names := map[string]string{}
	for path, entry := range manifest {
		name, err := RepositoryName(path)
		if err != nil {
			return nil, err
		}
		if previous, exists := names[name]; exists {
			return nil, fmt.Errorf("repository name collision: %q and %q map to %q", previous, path, name)
		}
		names[name] = path
		list := strings.Split(strings.TrimPrefix(path, "/"), "/")[0]
		if len(selected) > 0 {
			if _, ok := selected[list]; !ok {
				continue
			}
			selected[list] = true
		}
		result[path] = entry
	}
	for list, found := range selected {
		if !found {
			return nil, fmt.Errorf("selected inbox %q is absent from manifest", list)
		}
	}
	return result, nil
}
