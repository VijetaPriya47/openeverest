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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/openeverest/openeverest/v2/client"
	authcli "github.com/openeverest/openeverest/v2/pkg/cli/auth"
	"github.com/openeverest/openeverest/v2/pkg/cli/clienterr"
	"github.com/openeverest/openeverest/v2/pkg/output"
)

// UpdateOptions configures `instance update`.
type UpdateOptions struct {
	Name       string
	Namespace  string
	Cluster    string
	Context    string   // overrides the active context when set
	ValuesFile string   // path to a YAML values file with spec-level overrides
	Set        []string // dot-notation overrides e.g. "components.engine.replicas=3"; takes precedence over ValuesFile
	DryRun     bool     // preview the merged spec without writing it
}

// InstanceUpdater implements `instance update` business logic.
type InstanceUpdater struct {
	config Config
	l      *zap.SugaredLogger
}

// NewInstanceUpdater returns a new InstanceUpdater.
func NewInstanceUpdater(cfg Config, l *zap.SugaredLogger) *InstanceUpdater {
	iu := &InstanceUpdater{config: cfg, l: l.With("component", "instance-updater")}
	if cfg.Pretty {
		iu.l = zap.NewNop().Sugar()
	}
	return iu
}

// Run patches the instance spec with -f/--set and PUTs it back, retrying once on a 409 conflict. Fields not named by -f/--set keep their current value.
func (iu *InstanceUpdater) Run(ctx context.Context, opts UpdateOptions, cfgPath string) error {
	if opts.ValuesFile == "" && len(opts.Set) == 0 {
		return fmt.Errorf("at least one of --set or -f/--file is required")
	}

	c, err := authcli.NewAPIClient(authcli.Config{Pretty: iu.config.Pretty}, iu.l.Desugar().Sugar(), cfgPath, opts.Context)
	if err != nil {
		return err
	}

	overrides, err := buildSpecOverrides(opts.ValuesFile, opts.Set)
	if err != nil {
		return err
	}

	inst, err := iu.fetchInstance(ctx, c, opts)
	if err != nil {
		return err
	}

	if err := iu.validateSetComponents(ctx, c, opts, inst); err != nil {
		return err
	}

	merged, outgoing, err := prepareSpec(inst, overrides)
	if err != nil {
		return err
	}

	if opts.DryRun {
		return iu.emitDryRun(inst, outgoing, opts)
	}

	updated, err := iu.update(ctx, c, opts, inst, merged, overrides)
	if err != nil {
		return err
	}
	return iu.emitUpdated(updated, opts)
}

// validateSetComponents rejects --set paths naming an unknown component, the one typo the decoder's unknown-field check cannot catch.
func (iu *InstanceUpdater) validateSetComponents(ctx context.Context, c *client.ClientWithResponses, opts UpdateOptions, inst *client.Instance) error {
	if len(opts.Set) == 0 {
		return nil
	}
	provider := inst.Spec.ProviderRef.Name
	if provider == "" {
		return nil
	}

	resp, err := c.GetProviderWithResponse(ctx, opts.Cluster, provider)
	if err != nil || resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		// Fail open: the server validates component names too.
		iu.l.Warnf("could not fetch provider %q to validate component names, leaving it to the server", provider)
		return nil
	}

	var topology string
	if inst.Spec.Topology != nil && inst.Spec.Topology.Type != nil {
		topology = *inst.Spec.Topology.Type
	}
	return validateComponents(opts.Set, resp.JSON200, topology)
}

func (iu *InstanceUpdater) fetchInstance(ctx context.Context, c *client.ClientWithResponses, opts UpdateOptions) (*client.Instance, error) {
	resp, err := c.GetInstanceWithResponse(ctx, opts.Cluster, opts.Namespace, opts.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch instance %q: %w", opts.Name, err)
	}
	if resp.StatusCode() == http.StatusNotFound {
		return nil, fmt.Errorf("instance %q not found in namespace %q", opts.Name, opts.Namespace)
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected response fetching instance %q: %s", opts.Name, resp.Status())
	}
	return resp.JSON200, nil
}

// prepareSpec returns the map to PUT and the spec the server would store. The write and --dry-run both go through it, so they cannot accept different input.
func prepareSpec(inst *client.Instance, overrides map[string]any) (map[string]any, map[string]any, error) {
	merged, nulls, err := mergeSpec(inst, overrides)
	if err != nil {
		return nil, nil, err
	}
	outgoing, err := outgoingSpec(merged)
	if err != nil {
		return nil, nil, err
	}
	if err := checkUnset(outgoing, nulls); err != nil {
		return nil, nil, err
	}
	return merged, outgoing, nil
}

func mergeSpec(inst *client.Instance, overrides map[string]any) (map[string]any, []string, error) {
	current, err := specToMap(inst.Spec)
	if err != nil {
		return nil, nil, err
	}
	deepMerge(current, overrides)
	return current, stripNulls(current, ""), nil
}

// stripNulls deletes nil-valued keys instead of leaving them as JSON nulls, which would unset nothing, and returns their paths for checkUnset.
func stripNulls(m map[string]any, prefix string) []string {
	var paths []string
	for k, v := range m {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}
		switch tv := v.(type) {
		case nil:
			delete(m, k)
			paths = append(paths, path)
		case map[string]any:
			paths = append(paths, stripNulls(tv, path)...)
		}
	}
	sort.Strings(paths)
	return paths
}

// checkUnset rejects a null naming a field that cannot be unset, which would otherwise write its zero value: --set backup.enabled=null turning backups off.
func checkUnset(outgoing map[string]any, nulls []string) error {
	var stuck []string
	for _, p := range nulls {
		if _, found := lookupPath(outgoing, p); found {
			stuck = append(stuck, p)
		}
	}
	if len(stuck) == 0 {
		return nil
	}
	return fmt.Errorf("cannot unset %s with null: the field is required and always carries a value, so this would write its zero value rather than restore a default; set it explicitly instead",
		strings.Join(stuck, ", "))
}

func lookupPath(m map[string]any, path string) (any, bool) {
	var cur any = m
	for _, seg := range strings.Split(path, ".") {
		cm, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = cm[seg]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func specToMap(spec any) (map[string]any, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal instance spec: %w", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("failed to parse instance spec: %w", err)
	}
	return m, nil
}

// applyMergedSpec decodes into a zero spec, never into inst's own, so a replaced list cannot inherit fields from the elements it replaced.
// Unknown fields are rejected so a typo'd path fails instead of being dropped and reported as a successful update that changed nothing.
func applyMergedSpec(inst *client.Instance, merged map[string]any) error {
	b, err := json.Marshal(merged)
	if err != nil {
		return fmt.Errorf("failed to marshal merged spec: %w", err)
	}
	var decoded client.Instance
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&decoded.Spec); err != nil {
		return specDecodeError(err)
	}
	inst.Spec = decoded.Spec
	return nil
}

func specDecodeError(err error) error {
	const unknownField = "json: unknown field "
	if msg := err.Error(); strings.HasPrefix(msg, unknownField) {
		return fmt.Errorf("unknown spec field %s set via --set/-f; check the path for typos",
			strings.TrimPrefix(msg, unknownField))
	}
	return fmt.Errorf("failed to parse merged spec: %w", err)
}

// outgoingSpec renders merged as the PUT will carry it.
func outgoingSpec(merged map[string]any) (map[string]any, error) {
	var preview client.Instance
	if err := applyMergedSpec(&preview, merged); err != nil {
		return nil, err
	}
	return specToMap(preview.Spec)
}

func (iu *InstanceUpdater) update(ctx context.Context, c *client.ClientWithResponses, opts UpdateOptions, inst *client.Instance, merged map[string]any, overrides map[string]any) (*client.Instance, error) {
	for attempt := 0; ; attempt++ {
		updated, conflict, err := iu.tryUpdate(ctx, c, opts, inst, merged)
		if err != nil || !conflict {
			return updated, err
		}
		if attempt > 0 {
			return nil, fmt.Errorf("instance %q was concurrently modified twice; aborting after one retry, try again", opts.Name)
		}

		iu.l.Warnf("instance %q was concurrently modified, retrying once", opts.Name)
		if inst, err = iu.fetchInstance(ctx, c, opts); err != nil {
			return nil, err
		}
		if merged, _, err = prepareSpec(inst, overrides); err != nil {
			return nil, err
		}
	}
}

func (iu *InstanceUpdater) tryUpdate(ctx context.Context, c *client.ClientWithResponses, opts UpdateOptions, inst *client.Instance, merged map[string]any) (*client.Instance, bool, error) {
	if err := applyMergedSpec(inst, merged); err != nil {
		return nil, false, err
	}

	resp, err := c.UpdateInstanceWithResponse(ctx, opts.Cluster, opts.Namespace, opts.Name, *inst)
	if err != nil {
		return nil, false, fmt.Errorf("update instance request failed: %w", err)
	}

	switch resp.StatusCode() {
	case http.StatusOK:
		return resp.JSON200, false, nil
	case http.StatusConflict:
		return nil, true, nil
	case http.StatusNotFound:
		return nil, false, fmt.Errorf("instance %q not found in namespace %q", opts.Name, opts.Namespace)
	default:
		if msg, ok := clienterr.Message(resp.JSONDefault); ok {
			return nil, false, fmt.Errorf("server error: %s", msg)
		}
		return nil, false, fmt.Errorf("unexpected response updating instance %q: %s", opts.Name, resp.Status())
	}
}

// emitDryRun prints the current and would-be spec without writing. JSON mode emits a whole instance so the same jq works with or without --dry-run.
func (iu *InstanceUpdater) emitDryRun(inst *client.Instance, outgoing map[string]any, opts UpdateOptions) error {
	if !iu.config.Pretty {
		preview := *inst
		if err := applyMergedSpec(&preview, outgoing); err != nil {
			return err
		}
		return writeInstanceJSON(&preview)
	}

	current, err := specToMap(inst.Spec)
	if err != nil {
		return err
	}
	currentYAML, _ := yaml.Marshal(current)
	mergedYAML, _ := yaml.Marshal(outgoing)

	_, _ = fmt.Fprint(os.Stdout, output.Info("Dry run for instance %q, no changes written", opts.Name))
	_, _ = fmt.Fprintf(os.Stdout, "\n--- current spec ---\n%s\n--- would become ---\n%s", currentYAML, mergedYAML)
	return nil
}

func (iu *InstanceUpdater) emitUpdated(updated *client.Instance, opts UpdateOptions) error {
	if iu.config.Pretty {
		_, _ = fmt.Fprint(os.Stdout, output.Success("Instance %q updated in namespace %q", opts.Name, opts.Namespace))
		return nil
	}
	if updated == nil {
		return fmt.Errorf("instance %q was updated but the server returned an unreadable response body", opts.Name)
	}
	return writeInstanceJSON(updated)
}
