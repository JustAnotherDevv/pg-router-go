package config

import (
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// ErrConfigNotFound is returned when the requested config path does not exist.
var ErrConfigNotFound = errors.New("config file not found")

// Load reads + parses + validates a YAML config from the given path.
//
// Steps:
//  1. Read the file (ErrConfigNotFound if missing).
//  2. Strict YAML decode (`KnownFields(true)`) — typos in field names fail loudly.
//  3. Apply defaults so empty fields become live values.
//  4. Validate the resulting structure.
//
// Callers should treat a returned error as fatal at startup time.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrConfigNotFound, path)
		}
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()
	return LoadReader(f)
}

// LoadReader is the same as Load but takes a Reader. Used by tests +
// possible "read from stdin" flows.
func LoadReader(r io.Reader) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true) // reject unknown fields up-front
	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			// An empty file is fine — defaults will apply, then validation
			// will fail on missing required fields.
			cfg = Config{}
		} else {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	}
	applyDefaults(&cfg)
	if err := Validate(&cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}
