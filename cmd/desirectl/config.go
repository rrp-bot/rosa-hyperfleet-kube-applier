package main

import (
	"fmt"
	"os"
	"path/filepath"

	"sigs.k8s.io/yaml"
)

type Config struct {
	APIVersion     string         `json:"apiVersion"`
	Kind           string         `json:"kind"`
	CurrentContext string         `json:"current-context"`
	Contexts       []NamedContext `json:"contexts"`
}

type NamedContext struct {
	Name    string  `json:"name"`
	Context Context `json:"context"`
}

type Context struct {
	AWSRegion         string `json:"aws-region"`
	EndpointURL       string `json:"endpoint-url,omitempty"`
	ManagementCluster string `json:"management-cluster"`
	ClusterID         string `json:"cluster-id"`
	NodePool          string `json:"node-pool,omitempty"`
	Namespace         string `json:"namespace,omitempty"`
	SpecsTablePrefix  string `json:"specs-table,omitempty"`
	StatusTablePrefix string `json:"status-table,omitempty"`
}

func (c *Context) EffectiveSpecsPrefix() string {
	if c.SpecsTablePrefix != "" {
		return c.SpecsTablePrefix
	}
	return "mc-" + c.ManagementCluster + "-specs"
}

func (c *Context) EffectiveStatusPrefix() string {
	if c.StatusTablePrefix != "" {
		return c.StatusTablePrefix
	}
	return "mc-" + c.ManagementCluster + "-status"
}

func (c *Context) EffectiveNamespace(override string) string {
	if override != "" {
		return override
	}
	if c.Namespace != "" {
		return c.Namespace
	}
	return "default"
}

func DefaultConfigPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".desirectl", "config")
	}
	return filepath.Join(home, ".desirectl", "config")
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{
				APIVersion: "desirectl/v1",
				Kind:       "Config",
			}, nil
		}
		return nil, fmt.Errorf("reading config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	return &cfg, nil
}

func SaveConfig(path string, cfg *Config) error {
	if cfg.APIVersion == "" {
		cfg.APIVersion = "desirectl/v1"
	}
	if cfg.Kind == "" {
		cfg.Kind = "Config"
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating config directory: %w", err)
	}

	return os.WriteFile(path, data, 0o644)
}

func (c *Config) GetContext(name string) (*Context, bool) {
	for i := range c.Contexts {
		if c.Contexts[i].Name == name {
			return &c.Contexts[i].Context, true
		}
	}
	return nil, false
}

func (c *Config) SetContext(name string, ctx Context) {
	for i := range c.Contexts {
		if c.Contexts[i].Name == name {
			c.Contexts[i].Context = ctx
			return
		}
	}
	c.Contexts = append(c.Contexts, NamedContext{Name: name, Context: ctx})
}

func (c *Config) DeleteContext(name string) bool {
	for i := range c.Contexts {
		if c.Contexts[i].Name == name {
			c.Contexts = append(c.Contexts[:i], c.Contexts[i+1:]...)
			if c.CurrentContext == name {
				c.CurrentContext = ""
			}
			return true
		}
	}
	return false
}

func (c *Config) ResolveActiveContext(override string) (*Context, error) {
	name := override
	if name == "" {
		name = c.CurrentContext
	}
	if name == "" {
		return nil, fmt.Errorf("no context configured; run: desirectl config set-context <name> --aws-region=... --management-cluster=... --cluster-id=...")
	}
	ctx, ok := c.GetContext(name)
	if !ok {
		return nil, fmt.Errorf("context %q not found", name)
	}
	return ctx, nil
}
