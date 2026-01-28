package main

import (
	"os"

	"github.com/wesm/msgvault/cmd/msgvault/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
