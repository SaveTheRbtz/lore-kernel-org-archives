package lore

import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"maps"
	"regexp"
	"slices"
	"strings"
)

var ErrUnknownInbox = errors.New("selected inbox is absent from manifest")

var inboxNamePattern = regexp.MustCompile(`^` + listNamePattern + `$`)

// ParseLists validates the comma-separated inbox selection. An empty selection mirrors all inboxes.
func ParseLists(lists string) ([]string, error) {
	if lists == "" {
		return nil, nil
	}
	var names []string
	for list := range strings.SplitSeq(lists, ",") {
		name := strings.TrimSpace(list)
		if !inboxNamePattern.MatchString(name) {
			return nil, fmt.Errorf("invalid inbox name %q in -lists", list)
		}
		names = append(names, name)
	}
	return names, nil
}

func OrderedPaths(manifest Manifest) []string {
	return slices.SortedFunc(maps.Keys(manifest), func(a, b string) int {
		if order := cmp.Compare(manifest[b].Modified, manifest[a].Modified); order != 0 {
			return order
		}
		return cmp.Compare(a, b)
	})
}

// Filter the owned manifest in place. Check requested inbox names against every advertised path, including archives
// we skipped. Only supported epochs can appear in the result.
func selectManifest(manifest Manifest, advertised iter.Seq[string], lists []string, requireAll bool) (Manifest, error) {
	if len(lists) == 0 {
		return manifest, nil
	}
	selected := make(map[string]bool, len(lists))
	for _, list := range lists {
		selected[list] = false
	}
	for path := range advertised {
		list := InboxName(path)
		if _, ok := selected[list]; ok {
			selected[list] = true
		} else {
			delete(manifest, path)
		}
	}
	if requireAll {
		for list, found := range selected {
			if !found {
				return nil, fmt.Errorf("%w: %q", ErrUnknownInbox, list)
			}
		}
	}
	return manifest, nil
}
