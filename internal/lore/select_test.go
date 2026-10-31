package lore

import (
	"maps"
	"testing"
)

func TestSelectManifestKeepsAutomaticallyMirroredEpochs(t *testing.T) {
	cgroups := ManifestEntry{Modified: 7, Description: "cgroups"}
	oldEpoch := ManifestEntry{Modified: 11, Description: "old lkml epoch"}
	newEpoch := ManifestEntry{Modified: 13, Description: "new lkml epoch"}
	manifest := Manifest{
		"/cgroups/git/0.git": cgroups,
		"/lkml/git/0.git":    oldEpoch,
		"/lkml/git/2.git":    newEpoch,
	}
	for _, test := range []struct {
		lists string
		want  Manifest
	}{
		{"", Manifest{"/cgroups/git/0.git": cgroups, "/lkml/git/0.git": oldEpoch, "/lkml/git/2.git": newEpoch}},
		{"lkml", Manifest{"/lkml/git/0.git": oldEpoch, "/lkml/git/2.git": newEpoch}},
		{" cgroups, lkml ", Manifest{"/cgroups/git/0.git": cgroups, "/lkml/git/0.git": oldEpoch, "/lkml/git/2.git": newEpoch}},
	} {
		names, err := ParseLists(test.lists)
		if err != nil {
			t.Fatal(err)
		}
		got, err := selectManifest(maps.Clone(manifest), maps.Keys(manifest), names, true)
		if err != nil || !maps.Equal(got, test.want) {
			t.Errorf("select %q = %v, %v; want %v", test.lists, got, err, test.want)
		}
	}
}

func TestParseListsRejectsInvalidNames(t *testing.T) {
	for _, invalid := range []string{"-", " ", "lkml,", "lkml, ,git", "list/name"} {
		if names, err := ParseLists(invalid); err == nil || names != nil {
			t.Fatalf("ParseLists(%q) = %v, %v", invalid, names, err)
		}
	}
}

func TestSelectManifestRejectsUnknownInboxInitially(t *testing.T) {
	manifest := Manifest{"/lkml/git/0.git": {}}
	if got, err := selectManifest(manifest, maps.Keys(manifest), []string{"lkml", "missing"}, true); err == nil || got != nil {
		t.Fatalf("unknown inbox returned a partial selection: %v, %v", got, err)
	}
}

func TestSelectManifestReconcilesRemovedInboxesAndNewEpochs(t *testing.T) {
	manifest := Manifest{"/lkml/git/0.git": {}, "/lkml/git/2.git": {}, "/other/git/0.git": {}}
	got, err := selectManifest(manifest, maps.Keys(manifest), []string{"lkml", "cgroups"}, false)
	want := Manifest{"/lkml/git/0.git": {}, "/lkml/git/2.git": {}}
	if err != nil || !maps.Equal(got, want) {
		t.Fatalf("selection after cgroups removal = %v, %v; want %v", got, err, want)
	}
}
