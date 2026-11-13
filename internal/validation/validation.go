// Package validation provides optional per-job-type JSON Schema validation
// for job payloads. Schemas live as "<type>.json" files (JSON Schema draft
// 2020-12) in a configured directory; a type with a schema file gets its
// payload validated at submit time, a type without one is accepted as-is, so
// adoption is incremental per type.
package validation

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/message"
)

// Validator holds the compiled schema for each job type that has one. It is
// safe for concurrent use: the map is fixed once LoadDir returns.
type Validator struct {
	schemas map[string]*jsonschema.Schema
}

// LoadDir compiles every "<type>.json" file in dir into a Validator. A
// missing directory is not an error - it yields a Validator with no schemas,
// which accepts every payload (feature off). A malformed schema file IS an
// error: startup fails loudly rather than silently skipping validation for
// that type.
func LoadDir(dir string) (*Validator, error) {
	v := &Validator{schemas: make(map[string]*jsonschema.Schema)}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return v, nil
		}
		return nil, fmt.Errorf("reading payload schemas dir %q: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		jobType := strings.TrimSuffix(e.Name(), ".json")
		if jobType == "" {
			return nil, fmt.Errorf("payload schema file %q has no job type name", e.Name())
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("reading payload schema %s: %w", e.Name(), err)
		}
		doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(data)))
		if err != nil {
			return nil, fmt.Errorf("payload schema %s is not valid JSON: %w", e.Name(), err)
		}
		c := jsonschema.NewCompiler()
		if err := c.AddResource(jobType, doc); err != nil {
			return nil, fmt.Errorf("loading payload schema %s: %w", e.Name(), err)
		}
		sch, err := c.Compile(jobType)
		if err != nil {
			return nil, fmt.Errorf("payload schema %s does not compile: %w", e.Name(), err)
		}
		v.schemas[jobType] = sch
	}
	return v, nil
}

// Types returns the sorted job types that have a schema, for startup logging.
func (v *Validator) Types() []string {
	types := make([]string, 0, len(v.schemas))
	for t := range v.schemas {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// Validate checks payload against the schema registered for jobType. A type
// with no schema always passes. Violations are reported with the instance
// locations of up to 5 offending fields so the API caller can fix the payload
// without reading server logs.
func (v *Validator) Validate(jobType string, payload []byte) error {
	sch, ok := v.schemas[jobType]
	if !ok {
		return nil
	}
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(payload)))
	if err != nil {
		return fmt.Errorf("payload for type %q is not valid JSON: %w", jobType, err)
	}
	if err := sch.Validate(doc); err != nil {
		var ve *jsonschema.ValidationError
		if errors.As(err, &ve) {
			return fmt.Errorf("payload for type %q violates its schema: %s", jobType, flatten(ve, 5))
		}
		return fmt.Errorf("payload for type %q violates its schema: %v", jobType, err)
	}
	return nil
}

// flatten renders a ValidationError tree as a "; "-joined list of leaf
// failures, each "instanceLocation: message", capped at max leaves.
func flatten(ve *jsonschema.ValidationError, max int) string {
	p := message.NewPrinter(message.MatchLanguage("en"))
	msgs := make([]string, 0, max)
	truncated := false
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(msgs) >= max {
			truncated = true
			return
		}
		if len(e.Causes) == 0 {
			loc := "/" + strings.Join(e.InstanceLocation, "/")
			msgs = append(msgs, fmt.Sprintf("%s: %s", loc, e.ErrorKind.LocalizedString(p)))
			return
		}
		for _, c := range e.Causes {
			walk(c)
		}
	}
	walk(ve)
	if truncated {
		msgs = append(msgs, "(further violations omitted)")
	}
	return strings.Join(msgs, "; ")
}
