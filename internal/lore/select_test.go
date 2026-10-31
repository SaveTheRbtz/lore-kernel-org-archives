package lore

import (
	"encoding/json/jsontext"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"testing"
)

func TestManifestSelection(t *testing.T) {
	entry := jsontext.Value(`{"fingerprint":"` + manifestTestFingerprint + `","modified":0}`)
	raw := map[string]jsontext.Value{
		"/cgroups/git/0.git": entry,
		"/lkml/git/0.git":    entry,
		"/lkml/git/2.git":    entry,
		"/legacy":            jsontext.Value(`null`),
	}
	all := []string{"/cgroups/git/0.git", "/lkml/git/0.git", "/lkml/git/2.git"}
	lkml := all[1:]
	for _, tc := range []struct {
		lists      string
		requireAll bool
		want       []string
		wantErr    error
	}{
		{"", true, all, nil},
		{"lkml", true, lkml, nil},
		{" cgroups, lkml ", true, all, nil},
		{"lkml,lkml,legacy", true, lkml, nil},
		{"lkml,missing", true, nil, ErrUnknownInbox},
		{"lkml,missing", false, lkml, nil},
	} {
		names, err := ParseLists(tc.lists)
		if err != nil {
			t.Fatal(err)
		}
		got, err := manifestFromEntries(raw, slog.Default(), names, tc.requireAll)
		if !errors.Is(err, tc.wantErr) || (err != nil && got != nil) || !slices.Equal(slices.Sorted(maps.Keys(got)), tc.want) {
			t.Errorf("lists=%q requireAll=%t: got %v, %v; want %v, %v", tc.lists, tc.requireAll, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestSelectionValidatesUnselectedEntries(t *testing.T) {
	raw := map[string]jsontext.Value{
		"/cgroups/git/0.git": jsontext.Value(`{"fingerprint":"` + manifestTestFingerprint + `","modified":0}`),
		"/lkml/git/0.git":    jsontext.Value(`{}`),
	}
	if got, err := manifestFromEntries(raw, slog.Default(), map[string]bool{"cgroups": true}, true); got != nil || err == nil {
		t.Fatalf("malformed unselected archive: got %v, %v", got, err)
	}
}

func TestParseListsRejectsInvalidNames(t *testing.T) {
	for _, invalid := range []string{"-", " ", "lkml,", "lkml, ,git", "list/name"} {
		if names, err := ParseLists(invalid); err == nil || names != nil {
			t.Fatalf("ParseLists(%q) = %v, %v", invalid, names, err)
		}
	}
}
