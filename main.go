package main

import (
	"github.com/patrickjmcd/draino/cmd"
	logging "github.com/patrickjmcd/go-logger"
)

func init() {
	logging.SetupLogging()
}

func main() {
	cmd.Execute()
}
