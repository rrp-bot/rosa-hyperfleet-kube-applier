package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage desirectl configuration contexts",
	}

	cmd.AddCommand(
		newSetContextCmd(),
		newUseContextCmd(),
		newGetContextsCmd(),
		newCurrentContextCmd(),
		newDeleteContextCmd(),
	)

	return cmd
}

func newSetContextCmd() *cobra.Command {
	var awsRegion, endpointURL, mc, clusterID, nodePool, namespace, specsTable, statusTable string

	cmd := &cobra.Command{
		Use:   "set-context NAME",
		Short: "Create or update a named context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			path := DefaultConfigPath()

			cfg, err := LoadConfig(path)
			if err != nil {
				return err
			}

			existing, found := cfg.GetContext(name)
			ctx := Context{}
			if found {
				ctx = *existing
			}

			if cmd.Flags().Changed("aws-region") {
				ctx.AWSRegion = awsRegion
			}
			if cmd.Flags().Changed("endpoint-url") {
				ctx.EndpointURL = endpointURL
			}
			if cmd.Flags().Changed("management-cluster") {
				ctx.ManagementCluster = mc
			}
			if cmd.Flags().Changed("cluster-id") {
				ctx.ClusterID = clusterID
			}
			if cmd.Flags().Changed("node-pool") {
				ctx.NodePool = nodePool
			}
			if cmd.Flags().Changed("namespace") {
				ctx.Namespace = namespace
			}
			if cmd.Flags().Changed("specs-table") {
				ctx.SpecsTablePrefix = specsTable
			}
			if cmd.Flags().Changed("status-table") {
				ctx.StatusTablePrefix = statusTable
			}

			cfg.SetContext(name, ctx)

			if cfg.CurrentContext == "" {
				cfg.CurrentContext = name
			}

			if err := SaveConfig(path, cfg); err != nil {
				return err
			}

			if found {
				fmt.Printf("Context %q updated.\n", name)
			} else {
				fmt.Printf("Context %q created.\n", name)
			}
			if cfg.CurrentContext == name {
				fmt.Printf("Current context is %q.\n", name)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&awsRegion, "aws-region", "", "AWS region")
	cmd.Flags().StringVar(&endpointURL, "endpoint-url", "", "Optional endpoint URL override (e.g. http://localhost:4566)")
	cmd.Flags().StringVar(&mc, "management-cluster", "", "management cluster name")
	cmd.Flags().StringVar(&clusterID, "cluster-id", "", "cluster ID")
	cmd.Flags().StringVar(&nodePool, "node-pool", "", "node pool name")
	cmd.Flags().StringVar(&namespace, "namespace", "", "default namespace")
	cmd.Flags().StringVar(&specsTable, "specs-table", "", "DynamoDB specs table prefix (default: mc-{mc}-specs)")
	cmd.Flags().StringVar(&statusTable, "status-table", "", "DynamoDB status table prefix (default: mc-{mc}-status)")

	return cmd
}

func newUseContextCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "use-context NAME",
		Short: "Set the current context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			path := DefaultConfigPath()

			cfg, err := LoadConfig(path)
			if err != nil {
				return err
			}

			if _, ok := cfg.GetContext(name); !ok {
				return fmt.Errorf("context %q not found", name)
			}

			cfg.CurrentContext = name
			if err := SaveConfig(path, cfg); err != nil {
				return err
			}

			fmt.Printf("Switched to context %q.\n", name)
			return nil
		},
	}
}

func newGetContextsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get-contexts",
		Short: "List all contexts",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			path := DefaultConfigPath()
			cfg, err := LoadConfig(path)
			if err != nil {
				return err
			}

			if len(cfg.Contexts) == 0 {
				fmt.Println("No contexts configured.")
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintf(w, "CURRENT\tNAME\tAWS-REGION\tCLUSTER\tNAMESPACE\n")
			for _, nc := range cfg.Contexts {
				current := ""
				if nc.Name == cfg.CurrentContext {
					current = "*"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
					current, nc.Name, nc.Context.AWSRegion, nc.Context.ClusterID, nc.Context.Namespace)
			}
			w.Flush()
			return nil
		},
	}
}

func newCurrentContextCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "current-context",
		Short: "Display the current context",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			path := DefaultConfigPath()
			cfg, err := LoadConfig(path)
			if err != nil {
				return err
			}

			if cfg.CurrentContext == "" {
				return fmt.Errorf("no current context set")
			}

			fmt.Println(cfg.CurrentContext)
			return nil
		},
	}
}

func newDeleteContextCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete-context NAME",
		Short: "Delete a context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			path := DefaultConfigPath()

			cfg, err := LoadConfig(path)
			if err != nil {
				return err
			}

			if !cfg.DeleteContext(name) {
				return fmt.Errorf("context %q not found", name)
			}

			if err := SaveConfig(path, cfg); err != nil {
				return err
			}

			fmt.Printf("Context %q deleted.\n", name)
			return nil
		},
	}
}
