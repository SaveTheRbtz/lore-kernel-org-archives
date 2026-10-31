package lore

import (
	"maps"
	"testing"
)

func TestSelectManifestKeepsAllEpochsOfRequestedInbox(t *testing.T) {
	manifest := Manifest{"/cgroups/git/0.git": {}, "/lkml/git/0.git": {}, "/lkml/git/1.git": {}}
	for _, test := range []struct {
		lists string
		want  Manifest
	}{
		{"", manifest},
		{"lkml", Manifest{"/lkml/git/0.git": {}, "/lkml/git/1.git": {}}},
		{" cgroups, lkml ", manifest},
	} {
		got, err := SelectManifest(manifest, test.lists)
		if err != nil || !maps.Equal(got, test.want) {
			t.Errorf("select %q = %v, %v; want %v", test.lists, got, err, test.want)
		}
	}
}

func TestSelectManifestRejectsUnsafeSelection(t *testing.T) {
	for _, test := range []struct {
		name     string
		manifest Manifest
		lists    string
	}{
		{"missing inbox", Manifest{"/lkml/git/0.git": {}}, "missing"},
		{"invalid inbox", Manifest{"/lkml/git/0.git": {}}, "../lkml"},
		{"name collision", Manifest{"/Example/git/0.git": {}, "/example/git/0.git": {}}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := SelectManifest(test.manifest, test.lists); err == nil || got != nil {
				t.Fatalf("unsafe inventory accepted: %v, %v", got, err)
			}
		})
	}
}
