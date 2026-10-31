package lore

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
)

func OrderedPaths(manifest Manifest) []string {
	return slices.SortedFunc(maps.Keys(manifest), func(a, b string) int {
		if order := cmp.Compare(manifest[b].Modified, manifest[a].Modified); order != 0 {
			return order
		}
		return cmp.Compare(a, b)
	})
}

// requireAll rejects misspelled inbox names on the initial discovery. Later
// snapshots may remove inboxes while retaining the rest of the selection.
func SelectManifest(manifest Manifest, lists string, requireAll bool) (Manifest, error) {
	result := maps.Clone(manifest)
	// lkml.1 is maintained manually because GitHub rejects the NUL in
	// commit 299dbd50b6995c6debe2275f0df984ce697fb4cc.
	delete(result, "/lkml/git/1.git")
	if lists == "" {
		return result, nil
	}
	selected := map[string]bool{}
	for _, list := range strings.Split(lists, ",") {
		selected[strings.TrimSpace(list)] = false
	}
	for path := range manifest {
		list := strings.Split(strings.TrimPrefix(path, "/"), "/")[0]
		if _, ok := selected[list]; ok {
			selected[list] = true
		} else {
			delete(result, path)
		}
	}
	for list, found := range selected {
		if requireAll && !found {
			return nil, fmt.Errorf("selected inbox %q is absent from manifest", list)
		}
	}
	return result, nil
}
