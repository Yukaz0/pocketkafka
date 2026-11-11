module github.com/Yukaz0/pocketkafka

go 1.26

// Newest released Go 1.26.x patch (go.dev/dl); 1.26.11 fixes GO-2026-6859 in net/http. Pins the toolchain CI
// and contributors use without forcing a `go` directive bump to 1.27.
toolchain go1.26.11
