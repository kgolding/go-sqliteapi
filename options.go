package gdb

import (
	"log"
	"time"
)

type Option func(*Database) error

func Timeout(timeout time.Duration) Option {
	return func(d *Database) error {
		d.timeout = timeout
		return nil
	}
}

func YamlConfig(b []byte) Option {
	return func(d *Database) error {
		c, err := NewConfigFromYaml(b)
		if err != nil {
			return err
		}
		err = d.ApplyConfig(c, &ConfigOptions{
			RetainUnmanaged: true,
			// DryRun:          true,
			Logger: log.Default(),
		})
		if err != nil {
			return err
		}
		d.debugLog.Println("Config:\n" + c.String())
		d.config = c
		return nil
	}
}

type SimpleLogger interface {
	Println(v ...interface{})
	Printf(format string, v ...interface{})
}

func Log(logger SimpleLogger) Option {
	return func(d *Database) error {
		d.log = logger
		return nil
	}
}

func DebugLog(logger SimpleLogger) Option {
	return func(d *Database) error {
		d.debugLog = logger
		if d.log == nil {
			d.log = logger
		}
		return nil
	}
}
