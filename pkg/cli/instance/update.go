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
	"strconv"
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

// Updater implements `instance update` business logic.
type Updater struct {
	config Config
	l      *zap.SugaredLogger
}

// NewUpdater returns a new Updater.
func NewUpdater(cfg Config, l *zap.SugaredLogger) *Updater {
	iu := &Updater{config: cfg, l: l.With("component", "instance-updater")}
	if cfg.Pretty {
		iu.l = zap.NewNop().Sugar()
	}
	return iu
}

// Run patches the instance spec with -f/--set and PUTs it back, retrying once on a 409 conflict. Fields not named by -f/--set keep their current value.
func (iu *Updater) Run(ctx context.Context, opts UpdateOptions, cfgPath string) error {
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

	inst, err := getInstance(ctx, c, opts.Cluster, opts.Namespace, opts.Name)
	if err != nil {
		return err
	}

	if err := iu.validateSetComponents(ctx, c, opts, inst); err != nil {
		return err
	}

	prepared, err := prepareSpec(inst, overrides)
	if err != nil {
		return err
	}

	if opts.DryRun {
		return iu.emitDryRun(prepared, opts)
	}

	updated, err := iu.update(ctx, c, opts, prepared, overrides)
	if err != nil {
		return err
	}
	return iu.emitUpdated(updated, opts)
}

// validateSetComponents rejects --set paths naming an unknown component, the one typo the decoder's unknown-field check cannot catch.
func (iu *Updater) validateSetComponents(ctx context.Context, c *client.ClientWithResponses, opts UpdateOptions, inst *client.Instance) error {
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
		return nil //nolint:nilerr // a lookup failure must not block a valid update
	}

	var topology string
	if inst.Spec.Topology != nil && inst.Spec.Topology.Type != nil {
		topology = *inst.Spec.Topology.Type
	}
	return validateComponents(opts.Set, resp.JSON200, topology)
}

// getInstance is shared with instance status.
func getInstance(ctx context.Context, c *client.ClientWithResponses, cluster, namespace, name string) (*client.Instance, error) {
	resp, err := c.GetInstanceWithResponse(ctx, cluster, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch instance %q: %w", name, err)
	}
	if resp.StatusCode() == http.StatusNotFound {
		return nil, fmt.Errorf("instance %q not found in namespace %q", name, namespace)
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return nil, fmt.Errorf("unexpected response fetching instance %q: %s", name, resp.Status())
	}
	return resp.JSON200, nil
}

type preparedSpec struct {
	instance *client.Instance // spec merged, ready to PUT
	current  map[string]any   // spec before the merge
	outgoing map[string]any   // spec the server will store
}

// prepareSpec is the single path for the write and --dry-run, so the two cannot accept different input.
func prepareSpec(inst *client.Instance, overrides map[string]any) (*preparedSpec, error) {
	current, err := specToMap(inst.Spec)
	if err != nil {
		return nil, err
	}

	merged := deepCopyMap(current)
	deepMerge(merged, overrides)
	nulls := stripNulls(merged, "")

	prepared := *inst
	if err := applyMergedSpec(&prepared, merged); err != nil {
		return nil, err
	}
	outgoing, err := specToMap(prepared.Spec)
	if err != nil {
		return nil, err
	}
	if err := checkUnset(outgoing, nulls); err != nil {
		return nil, err
	}
	return &preparedSpec{instance: &prepared, current: current, outgoing: outgoing}, nil
}

// deepCopyMap copies only the map structure; list values, and any maps inside them, stay shared with the source.
func deepCopyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if child, ok := v.(map[string]any); ok {
			out[k] = deepCopyMap(child)
			continue
		}
		out[k] = v
	}
	return out
}

// stripNulls deletes nil-valued keys instead of leaving them as JSON nulls, which would unset nothing, and returns their paths for checkUnset.
// It recurses into list elements too, so a null under storages[0] is caught the same way.
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
		case []any:
			for i, elem := range tv {
				if child, ok := elem.(map[string]any); ok {
					paths = append(paths, stripNulls(child, fmt.Sprintf("%s[%d]", path, i))...)
				}
			}
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
	for seg := range strings.SplitSeq(path, ".") {
		key, idx, indexed := parseIndexedSegment(seg)
		cm, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = cm[key]; !ok {
			return nil, false
		}
		if indexed {
			list, ok := cur.([]any)
			if !ok || idx >= len(list) {
				return nil, false
			}
			cur = list[idx]
		}
	}
	return cur, true
}

// parseIndexedSegment reads the "storages[0]" form stripNulls records.
func parseIndexedSegment(seg string) (string, int, bool) {
	open := strings.IndexByte(seg, '[')
	if open < 0 || !strings.HasSuffix(seg, "]") {
		return seg, 0, false
	}
	idx, err := strconv.Atoi(seg[open+1 : len(seg)-1])
	if err != nil || idx < 0 {
		return seg, 0, false
	}
	return seg[:open], idx, true
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

func (iu *Updater) update(ctx context.Context, c *client.ClientWithResponses, opts UpdateOptions, prepared *preparedSpec, overrides map[string]any) (*client.Instance, error) {
	for attempt := 0; ; attempt++ {
		updated, conflict, err := iu.tryUpdate(ctx, c, opts, prepared.instance)
		if err != nil || !conflict {
			return updated, err
		}
		if attempt > 0 {
			return nil, fmt.Errorf("instance %q was concurrently modified twice; aborting after one retry, try again", opts.Name)
		}

		iu.l.Warnf("instance %q was concurrently modified, retrying once", opts.Name)
		fresh, err := getInstance(ctx, c, opts.Cluster, opts.Namespace, opts.Name)
		if err != nil {
			return nil, err
		}
		if prepared, err = prepareSpec(fresh, overrides); err != nil {
			return nil, err
		}
	}
}

func (iu *Updater) tryUpdate(ctx context.Context, c *client.ClientWithResponses, opts UpdateOptions, inst *client.Instance) (*client.Instance, bool, error) {
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
func (iu *Updater) emitDryRun(prepared *preparedSpec, opts UpdateOptions) error {
	if !iu.config.Pretty {
		return writeInstanceJSON(prepared.instance)
	}

	currentYAML, _ := yaml.Marshal(prepared.current)
	mergedYAML, _ := yaml.Marshal(prepared.outgoing)

	_, _ = fmt.Fprint(os.Stdout, output.Info("Dry run for instance %q, no changes written", opts.Name))
	_, _ = fmt.Fprintf(os.Stdout, "\n--- current spec ---\n%s\n--- would become ---\n%s", currentYAML, mergedYAML)
	return nil
}

func (iu *Updater) emitUpdated(updated *client.Instance, opts UpdateOptions) error {
	if iu.config.Pretty {
		_, _ = fmt.Fprint(os.Stdout, output.Success("Instance %q updated in namespace %q", opts.Name, opts.Namespace))
		return nil
	}
	if updated == nil {
		return fmt.Errorf("instance %q was updated but the server returned an unreadable response body", opts.Name)
	}
	return writeInstanceJSON(updated)
}
