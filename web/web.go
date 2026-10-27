package web

import (
	"embed"
	"io/fs"
)

//go:embed dist/*
var embeddedAssets embed.FS

func FS() fs.FS {
	sub, err := fs.Sub(embeddedAssets, "dist")
	if err != nil {
		panic(err)
	}
	return sub
}
