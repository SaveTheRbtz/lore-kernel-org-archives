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
		got, err := SelectManifest(manifest, test.lists, true)
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
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := SelectManifest(test.manifest, test.lists, true); err == nil || got != nil {
				t.Fatalf("unsafe inventory accepted: %v, %v", got, err)
			}
		})
	}
}

func TestSelectManifestReconcilesRemovedInboxesAndNewEpochs(t *testing.T) {
	manifest := Manifest{"/lkml/git/0.git": {}, "/lkml/git/1.git": {}, "/other/git/0.git": {}}
	got, err := SelectManifest(manifest, "lkml,cgroups", false)
	want := Manifest{"/lkml/git/0.git": {}, "/lkml/git/1.git": {}}
	if err != nil || !maps.Equal(got, want) {
		t.Fatalf("selection after cgroups removal = %v, %v; want %v", got, err, want)
	}
}
