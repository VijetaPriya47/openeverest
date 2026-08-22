// everest
// Copyright (C) 2026 The OpenEverest Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package instance

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/openeverest/openeverest/v2/pkg/cli"
	"github.com/openeverest/openeverest/v2/pkg/cli/config"
	instancecli "github.com/openeverest/openeverest/v2/pkg/cli/instance"
	"github.com/openeverest/openeverest/v2/pkg/logger"
	"github.com/openeverest/openeverest/v2/pkg/output"
)

var (
	updateCmd = &cobra.Command{
		Use:   "update [flags]",
		Args:  cobra.NoArgs,
		Short: "Update an instance",
		Long: `Patch an existing instance's spec through the Everest API.

The name and namespace flags are required. Changes are expressed with --set
dot-notation paths and/or a -f values file, using the same merge rules as
'instance create' (--set wins over -f). This is a patch, not a
desired-state replacement: every field not named by --set/-f keeps its
current value. At least one of --set/-f is required.

--set and -f paths are validated against the instance schema before anything
is written, so a misspelled field or component name fails instead of being
silently ignored.

Use --set path=null to drop an optional field from the spec, letting
server-side defaulting reapply. Only optional fields can be unset: a required
field has no empty form, so nulling it would quietly write its zero value
instead of restoring a default (--set backup.enabled=null would turn backups
off). Those are rejected, set the value you want explicitly.`,
		Example: `  # Scale the engine
  everestctl instance update --name my-mongo --namespace everest \
    --set components.engine.replicas=5

  # Preview a change before applying it
  everestctl instance update --name my-mongo --namespace everest \
    --set components.engine.storage.size=100Gi --dry-run

  # Remove an override, falling back to the server-side default
  everestctl instance update --name my-mongo --namespace everest \
    --set components.proxy.parameters=null

  # Bulk changes from a values file (merged, absent fields stay untouched)
  everestctl instance update --name my-mongo --namespace everest -f new-values.yaml`,
		PreRun: updatePreRun,
		Run:    updateRun,
	}
	updateCfg  = &instancecli.Config{}
	updateOpts = &instancecli.UpdateOptions{}
)

func init() {
	updateCmd.Flags().StringVar(&updateOpts.Name, cli.FlagInstanceName, "", "Instance name (required)")
	updateCmd.Flags().StringVarP(&updateOpts.Namespace, cli.FlagInstanceNamespace, "n", "", "Namespace the instance is in (required)")
	updateCmd.Flags().StringVar(&updateOpts.Cluster, cli.FlagInstanceCluster, "main", "Cluster name")
	updateCmd.Flags().StringVar(&updateOpts.Context, cli.FlagInstanceContext, "", "Context to use (default: current context)")
	updateCmd.Flags().StringVarP(&updateOpts.ValuesFile, cli.FlagInstanceFile, "f", "", "Path to a YAML file with spec overrides (--set takes precedence)")
	updateCmd.Flags().StringArrayVar(&updateOpts.Set, cli.FlagInstanceSet, nil, "Set a spec field: --set components.engine.replicas=3 or --set backup.enabled=true (repeatable)")
	updateCmd.Flags().BoolVar(&updateOpts.DryRun, cli.FlagInstanceDryRun, false, "Preview the merged spec without writing it")

	_ = updateCmd.MarkFlagRequired(cli.FlagInstanceName)
	_ = updateCmd.MarkFlagRequired(cli.FlagInstanceNamespace)
}

func updatePreRun(cmd *cobra.Command, _ []string) { //nolint:revive
	updateCfg.Pretty = !(cmd.Flag(cli.FlagVerbose).Changed || cmd.Flag(cli.FlagJSON).Changed)
}

func updateRun(cmd *cobra.Command, _ []string) {
	cfgPath, err := config.DefaultPath()
	if err != nil {
		output.PrintError(err, logger.GetLogger(), updateCfg.Pretty)
		os.Exit(1)
	}

	iu := instancecli.NewInstanceUpdater(*updateCfg, logger.GetLogger())
	if err := iu.Run(cmd.Context(), *updateOpts, cfgPath); err != nil {
		output.PrintError(err, logger.GetLogger(), updateCfg.Pretty)
		os.Exit(1)
	}
}

// GetUpdateCmd returns the update command.
func GetUpdateCmd() *cobra.Command {
	return updateCmd
}
