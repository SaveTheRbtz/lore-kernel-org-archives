package lore

import "testing"

func TestSelectManifest(t *testing.T) {
	manifest := Manifest{"/cgroups/git/0.git": {}, "/lkml/git/0.git": {}, "/lkml/git/1.git": {}}
	selected, err := SelectManifest(manifest, "lkml")
	if err != nil || len(selected) != 2 {
		t.Fatalf("selected %v, error %v", selected, err)
	}
	if _, err := SelectManifest(manifest, "missing"); err == nil {
		t.Fatal("missing inbox accepted")
	}
	if _, err := SelectManifest(manifest, "../lkml"); err == nil {
		t.Fatal("invalid inbox accepted")
	}
	if _, err := SelectManifest(Manifest{"/Example/git/0.git": {}, "/example/git/0.git": {}}, ""); err == nil {
		t.Fatal("case-insensitive name collision accepted")
	}
}

type delayedError struct{}
