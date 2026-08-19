// Package web embeds the go-kafka-neu frontend (single-page dashboard) so it
// can be served directly from the broker binary via //go:embed. The embedded
// assets live in web/dist.
package web

import (
	"embed"
	"io/fs"
)

//go:embed dist/*
var embeddedAssets embed.FS

// FS returns the embedded frontend filesystem rooted at dist.
func FS() fs.FS {
	sub, err := fs.Sub(embeddedAssets, "dist")
	if err != nil {
		panic(err)
	}
	return sub
}
