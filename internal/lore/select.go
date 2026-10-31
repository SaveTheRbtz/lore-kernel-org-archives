package lore

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

var ErrUnknownInbox = errors.New("selected inbox is absent from manifest")

var inboxNamePattern = regexp.MustCompile(`^` + listNamePattern + `$`)

// ParseLists validates the comma-separated inbox selection. An empty selection mirrors all inboxes.
func ParseLists(lists string) (map[string]bool, error) {
	if lists == "" {
		return nil, nil
	}
	names := make(map[string]bool)
	for list := range strings.SplitSeq(lists, ",") {
		name := strings.TrimSpace(list)
		if !inboxNamePattern.MatchString(name) {
			return nil, fmt.Errorf("invalid inbox name %q in -lists", list)
		}
		names[name] = true
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
