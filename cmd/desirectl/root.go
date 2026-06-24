package main

import (
	"github.com/spf13/cobra"
)

var (
	flagContext   string
	flagNamespace string
	flagOutput    string
	flagTimeout   string
)

func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "desirectl",
		Short: "kubectl-like CLI for managing Kubernetes resources via kube-applier desires",
		Long: `desirectl provides a kubectl-compatible interface for managing Kubernetes
resources through the kube-applier-aws desire system. Resources are applied,
read, and deleted using familiar kubectl commands, while the desire layer
handles the actual cluster operations.`,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	root.PersistentFlags().StringVar(&flagContext, "context", "", "override current context")
	root.PersistentFlags().StringVarP(&flagNamespace, "namespace", "n", "", "namespace (overrides context default)")
	root.PersistentFlags().StringVarP(&flagOutput, "output", "o", "table", "output format: table, json, yaml, wide")
	root.PersistentFlags().StringVar(&flagTimeout, "timeout", "30s", "timeout for waiting on status (e.g. 30s, 1m)")

	root.AddCommand(
		newApplyCmd(),
		newGetCmd(),
		newDeleteCmd(),
		newConfigCmd(),
		newVersionCmd(),
	)

	return root
}
