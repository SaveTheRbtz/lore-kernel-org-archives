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

var inboxName = regexp.MustCompile(`^` + listNamePattern + `$`)

func ValidateLists(lists string) error {
	if lists == "" {
		return nil
	}
	for list := range strings.SplitSeq(lists, ",") {
		if !inboxName.MatchString(strings.TrimSpace(list)) {
			return fmt.Errorf("invalid inbox name %q in -lists", list)
		}
	}
	return nil
}

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
			return nil, fmt.Errorf("%w: %q", ErrUnknownInbox, list)
		}
	}
	return result, nil
}
