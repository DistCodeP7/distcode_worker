package log

import (
	"os"

	"github.com/sirupsen/logrus"
)

var Logger = logrus.New()

func Init(level logrus.Level, pretty bool) {
	Logger.Out = os.Stdout

	if pretty {
		Logger.Formatter = &logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "02-01-2006 15:04:05",
			ForceColors:     true,
		}
	} else {
		// default JSON output
		Logger.Formatter = &logrus.JSONFormatter{}
	}

	Logger.SetLevel(level)
}
