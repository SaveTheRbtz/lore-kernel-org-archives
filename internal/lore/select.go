package lore

import (
	"cmp"
	"fmt"
	"iter"
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

// Check requested inbox names against every advertised path, including archives
// we skipped. Only supported epochs can appear in the result.
func selectManifest(manifest Manifest, advertised iter.Seq[string], lists string, requireAll bool) (Manifest, error) {
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
	for path := range advertised {
		list, _, _ := strings.Cut(path[1:], "/")
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
