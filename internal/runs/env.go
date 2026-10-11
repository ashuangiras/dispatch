package runs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// EnvSpec describes a child environment. The base is the server's own
// environment when Inherit is true (the default) and empty otherwise; keys in
// Unset are then removed and Set is applied. A key may not be in both.
type EnvSpec struct {
	Inherit *bool
	Set     map[string]string
	Unset   []string
}

// EnvRecord is what Dispatch keeps and reports about an environment: never values.
type EnvRecord struct {
	Inherit bool     `json:"inherit"`
	SetKeys []string `json:"set_keys,omitempty"`
	Unset   []string `json:"unset,omitempty"`
}

func validKey(k string) error {
	if k == "" || strings.ContainsAny(k, "=\x00") {
		return fmt.Errorf("invalid environment variable name %q", k)
	}
	return nil
}

func (e EnvSpec) validate() error {
	for k, v := range e.Set {
		if err := validKey(k); err != nil {
			return err
		}
		if strings.ContainsRune(v, 0) {
			return fmt.Errorf("environment variable %s: value contains a NUL byte", k)
		}
	}
	for _, k := range e.Unset {
		if err := validKey(k); err != nil {
			return err
		}
		if _, ok := e.Set[k]; ok {
			return fmt.Errorf("environment variable %s is in both set and unset", k)
		}
	}
	return nil
}

func (e EnvSpec) inherit() bool { return e.Inherit == nil || *e.Inherit }

func (e EnvSpec) record() EnvRecord {
	r := EnvRecord{Inherit: e.inherit(), Unset: append([]string(nil), e.Unset...)}
	for k := range e.Set {
		r.SetKeys = append(r.SetKeys, k)
	}
	sort.Strings(r.SetKeys)
	sort.Strings(r.Unset)
	return r
}

// build returns the child's environment as sorted KEY=VALUE pairs.
func (e EnvSpec) build(base []string) []string {
	m := map[string]string{}
	if e.inherit() {
		for _, kv := range base {
			if k, v, ok := strings.Cut(kv, "="); ok {
				m[k] = v
			}
		}
	}
	for _, k := range e.Unset {
		delete(m, k)
	}
	for k, v := range e.Set {
		m[k] = v
	}
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

func lookupEnv(env []string, key string) (string, bool) {
	for _, kv := range env {
		if k, v, ok := strings.Cut(kv, "="); ok && k == key {
			return v, true
		}
	}
	return "", false
}

// resolveExecutable finds command the way the child would expect: a name with
// a '/' is a path (relative to cwd); a bare name is searched in the child's own
// PATH, skipping relative PATH entries.
func resolveExecutable(command, cwd string, env []string) (string, error) {
	if strings.Contains(command, "/") {
		p := command
		if !filepath.IsAbs(p) {
			p = filepath.Join(cwd, p)
		}
		if err := checkExecutable(p); err != nil {
			return "", err
		}
		return p, nil
	}
	path, ok := lookupEnv(env, "PATH")
	if !ok {
		return "", fmt.Errorf("%q not found: the child environment has no PATH; give a path containing '/'", command)
	}
	for _, dir := range filepath.SplitList(path) {
		if dir == "" || !filepath.IsAbs(dir) {
			continue
		}
		p := filepath.Join(dir, command)
		if checkExecutable(p) == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("%q not found in the child's PATH", command)
}

func checkExecutable(p string) error {
	fi, err := os.Stat(p)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		return fmt.Errorf("%s is a directory", p)
	}
	if fi.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%s: %w", p, os.ErrPermission)
	}
	return nil
}

func resolveCwd(cwd string) (string, error) {
	if cwd == "" {
		return os.Getwd()
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", err
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return "", fmt.Errorf("working directory: %w", err)
	}
	if !fi.IsDir() {
		return "", errors.New("working directory " + abs + " is not a directory")
	}
	return abs, nil
}
