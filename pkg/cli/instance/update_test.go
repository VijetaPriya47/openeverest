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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/openeverest/openeverest/v2/client"
)

func newUpdateServer(t *testing.T, getHandler, putHandler http.HandlerFunc) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	path := "/v1/clusters/main/namespaces/everest/instances/my-db"
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getHandler(w, r)
		case http.MethodPut:
			if putHandler == nil {
				t.Fatal("PUT should not have been called")
				return
			}
			putHandler(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return httptest.NewServer(mux)
}

func newUpdateServerWithProvider(t *testing.T, getHandler, putHandler http.HandlerFunc) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/clusters/main/providers/psmdb", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(psmdbProvider())
	})
	mux.HandleFunc("/v1/clusters/main/namespaces/everest/instances/my-db", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getHandler(w, r)
		case http.MethodPut:
			if putHandler == nil {
				t.Fatal("PUT should not have been called")
				return
			}
			putHandler(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return httptest.NewServer(mux)
}

func baseUpdateOpts() UpdateOptions {
	return UpdateOptions{Name: "my-db", Namespace: "everest", Cluster: "main"}
}

func instanceJSON(t *testing.T, resourceVersion string, spec map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"name": "my-db", "namespace": "everest", "resourceVersion": resourceVersion},
		"spec":     spec,
	})
	require.NoError(t, err)
	return b
}

func writeJSONBody(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// asMap and asSlice use assert, not require: require's FailNow is invalid off
// the test goroutine, and these run inside http handlers.
func asMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	assert.True(t, ok, "expected a map, got %T", v)
	return m
}

func asSlice(t *testing.T, v any) []any {
	t.Helper()
	s, ok := v.([]any)
	assert.True(t, ok, "expected a slice, got %T", v)
	return s
}

func decodeSpec(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	var full map[string]any
	require.NoError(t, json.Unmarshal(b, &full))
	spec, ok := full["spec"].(map[string]any)
	require.True(t, ok, "request body has no spec: %s", string(b))
	return spec
}

func TestUpdate_NoOp_Rejected(t *testing.T) {
	t.Parallel()

	iu := NewUpdater(Config{}, zap.NewNop().Sugar())
	err := iu.Run(context.Background(), baseUpdateOpts(), filepath.Join(t.TempDir(), "config.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--set")
}

func TestUpdate_HappyPath_PatchNotReplace(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"version":     "8.0",
		"components": map[string]any{
			"engine": map[string]any{"replicas": 3},
			"proxy":  map[string]any{"replicas": 1},
		},
	}

	srv := newUpdateServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			writeJSONBody(w, instanceJSON(t, "1", currentSpec))
		},
		func(w http.ResponseWriter, r *http.Request) {
			spec := decodeSpec(t, r)
			components := asMap(t, spec["components"])
			engine := asMap(t, components["engine"])
			proxy := asMap(t, components["proxy"])
			assert.InDelta(t, 5, engine["replicas"], 0, "changed field should be updated")
			assert.InDelta(t, 1, proxy["replicas"], 0, "untouched field should keep its current value")
			assert.Equal(t, "8.0", spec["version"], "untouched field should keep its current value")
			writeJSONBody(w, instanceJSON(t, "2", spec))
		},
	)
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

	opts := baseUpdateOpts()
	opts.Set = []string{"components.engine.replicas=5"}

	iu := NewUpdater(Config{}, zap.NewNop().Sugar())
	err := iu.Run(context.Background(), opts, cfgPath)
	require.NoError(t, err)
}

func TestUpdate_SetNull_UnsetsField(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"version":     "8.0",
	}

	srv := newUpdateServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			writeJSONBody(w, instanceJSON(t, "1", currentSpec))
		},
		func(w http.ResponseWriter, r *http.Request) {
			spec := decodeSpec(t, r)
			_, present := spec["version"]
			assert.False(t, present, "null-set field should be absent from the outgoing spec")
			writeJSONBody(w, instanceJSON(t, "2", spec))
		},
	)
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

	opts := baseUpdateOpts()
	opts.Set = []string{"version=null"}

	iu := NewUpdater(Config{}, zap.NewNop().Sugar())
	err := iu.Run(context.Background(), opts, cfgPath)
	require.NoError(t, err)
}

func TestUpdate_ValuesFile_Merge(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"components":  map[string]any{"engine": map[string]any{"replicas": 3}},
	}

	for _, tc := range []struct {
		name         string
		extraSet     []string
		wantReplicas float64
	}{
		{"file only", nil, 7},
		{"set overrides file", []string{"components.engine.replicas=9"}, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newUpdateServer(t,
				func(w http.ResponseWriter, _ *http.Request) {
					writeJSONBody(w, instanceJSON(t, "1", currentSpec))
				},
				func(w http.ResponseWriter, r *http.Request) {
					spec := decodeSpec(t, r)
					engine := asMap(t, asMap(t, spec["components"])["engine"])
					assert.InDelta(t, tc.wantReplicas, engine["replicas"], 0, "--set should win over -f")
					writeJSONBody(w, instanceJSON(t, "2", spec))
				},
			)
			defer srv.Close()

			cfgPath := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

			valuesPath := filepath.Join(t.TempDir(), "values.yaml")
			require.NoError(t, os.WriteFile(valuesPath, []byte("components:\n  engine:\n    replicas: 7\n"), 0o600))

			opts := baseUpdateOpts()
			opts.ValuesFile = valuesPath
			opts.Set = tc.extraSet

			iu := NewUpdater(Config{}, zap.NewNop().Sugar())
			err := iu.Run(context.Background(), opts, cfgPath)
			require.NoError(t, err)
		})
	}
}

func TestUpdate_NotFound(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{"providerRef": map[string]any{"name": "psmdb"}}
	notFound := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }
	getOK := func(w http.ResponseWriter, _ *http.Request) { writeJSONBody(w, instanceJSON(t, "1", currentSpec)) }

	for _, tc := range []struct {
		name       string
		getHandler http.HandlerFunc
		putHandler http.HandlerFunc
	}{
		{"get", notFound, nil},
		{"put", getOK, notFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newUpdateServer(t, tc.getHandler, tc.putHandler)
			defer srv.Close()

			cfgPath := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

			opts := baseUpdateOpts()
			opts.Set = []string{"components.engine.replicas=5"}

			iu := NewUpdater(Config{}, zap.NewNop().Sugar())
			err := iu.Run(context.Background(), opts, cfgPath)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})
	}
}

func TestUpdate_ConflictThenSuccess(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"components":  map[string]any{"engine": map[string]any{"replicas": 3}},
	}

	getCalls, putCalls := 0, 0
	srv := newUpdateServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			getCalls++
			writeJSONBody(w, instanceJSON(t, "1", currentSpec))
		},
		func(w http.ResponseWriter, r *http.Request) {
			putCalls++
			if putCalls == 1 {
				w.WriteHeader(http.StatusConflict)
				return
			}
			spec := decodeSpec(t, r)
			engine := asMap(t, asMap(t, spec["components"])["engine"])
			assert.InDelta(t, 5, engine["replicas"], 0, "the retry should reapply the same override")
			writeJSONBody(w, instanceJSON(t, "2", spec))
		},
	)
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

	opts := baseUpdateOpts()
	opts.Set = []string{"components.engine.replicas=5"}

	iu := NewUpdater(Config{}, zap.NewNop().Sugar())
	err := iu.Run(context.Background(), opts, cfgPath)
	require.NoError(t, err)
	assert.Equal(t, 2, getCalls, "should re-fetch once after the conflict")
	assert.Equal(t, 2, putCalls, "should retry the PUT exactly once")
}

func TestUpdate_ConflictTwice_Fails(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{"providerRef": map[string]any{"name": "psmdb"}}

	putCalls := 0
	srv := newUpdateServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			writeJSONBody(w, instanceJSON(t, "1", currentSpec))
		},
		func(w http.ResponseWriter, _ *http.Request) {
			putCalls++
			w.WriteHeader(http.StatusConflict)
		},
	)
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

	opts := baseUpdateOpts()
	opts.Set = []string{"components.engine.replicas=5"}

	iu := NewUpdater(Config{}, zap.NewNop().Sugar())
	err := iu.Run(context.Background(), opts, cfgPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "concurrently modified twice")
	assert.Equal(t, 2, putCalls, "should not retry more than once")
}

// --set path=null must preview the field as gone, not present-and-null.
//
// Not parallel: captureStdout mutates global os.Stdout.
//
//nolint:paralleltest // mutates global os.Stdout; must run serially
func TestUpdate_DryRun_NullShowsFieldRemoved(t *testing.T) {
	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"version":     "8.0",
	}

	srv := newUpdateServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			writeJSONBody(w, instanceJSON(t, "1", currentSpec))
		},
		nil, // PUT must not be called
	)
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

	opts := baseUpdateOpts()
	opts.Set = []string{"version=null"}
	opts.DryRun = true

	stdout := captureStdout(t, func() {
		iu := NewUpdater(Config{}, zap.NewNop().Sugar())
		require.NoError(t, iu.Run(context.Background(), opts, cfgPath))
	})

	var previewed map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &previewed))
	_, present := previewed["version"]
	assert.False(t, present, "dry-run must preview the field as removed, not null: %s", stdout)
}

// A typo'd --set path must be reported, not dropped silently.
func TestUpdate_UnknownField_Rejected(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"components":  map[string]any{"engine": map[string]any{"replicas": 3}},
	}

	for _, tc := range []struct {
		name   string
		dryRun bool
		set    string
	}{
		{"top-level field, real update", false, "totallyMadeUpField=123"},
		{"top-level field, dry run", true, "totallyMadeUpField=123"},
		{"nested field, real update", false, "components.engine.replicaz=5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newUpdateServer(t,
				func(w http.ResponseWriter, _ *http.Request) {
					writeJSONBody(w, instanceJSON(t, "1", currentSpec))
				},
				nil, // a rejected path must never reach the PUT
			)
			defer srv.Close()

			cfgPath := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

			opts := baseUpdateOpts()
			opts.Set = []string{tc.set}
			opts.DryRun = tc.dryRun

			iu := NewUpdater(Config{}, zap.NewNop().Sugar())
			err := iu.Run(context.Background(), opts, cfgPath)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown spec field")
			assert.Contains(t, err.Error(), "--set")
		})
	}
}

// A replaced list must not carry fields from the old elements.
func TestUpdate_ReplacedListDoesNotLeakOldFields(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"backup": map[string]any{
			"enabled":  true,
			"classRef": map[string]any{"name": "c"},
			"storages": []any{map[string]any{
				"storageRef": map[string]any{"name": "s1"},
				"schedules": []any{map[string]any{
					"name": "nightly", "cron": "0 0 * * *", "enabled": true,
				}},
			}},
		},
	}

	srv := newUpdateServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			writeJSONBody(w, instanceJSON(t, "1", currentSpec))
		},
		func(w http.ResponseWriter, r *http.Request) {
			spec := decodeSpec(t, r)
			storages := asSlice(t, asMap(t, spec["backup"])["storages"])
			if !assert.Len(t, storages, 1) {
				return
			}
			only := asMap(t, storages[0])
			assert.Equal(t, "s2", asMap(t, only["storageRef"])["name"])
			_, leaked := only["schedules"]
			assert.False(t, leaked, "replacement element must not inherit the old element's schedules: %v", only)
			writeJSONBody(w, instanceJSON(t, "2", spec))
		},
	)
	defer srv.Close()

	valuesPath := filepath.Join(t.TempDir(), "values.yaml")
	require.NoError(t, os.WriteFile(valuesPath,
		[]byte("backup:\n  storages:\n    - storageRef:\n        name: s2\n"), 0o600))

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

	opts := baseUpdateOpts()
	opts.ValuesFile = valuesPath

	iu := NewUpdater(Config{}, zap.NewNop().Sugar())
	require.NoError(t, iu.Run(context.Background(), opts, cfgPath))
}

// Preview and write must agree on an unset.
//
// Not parallel: captureStdout mutates global os.Stdout.
//
//nolint:paralleltest // mutates global os.Stdout; must run serially
func TestUpdate_DryRunMatchesWrite(t *testing.T) {
	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"version":     "8.0",
		"backup": map[string]any{
			"enabled":  true,
			"classRef": map[string]any{"name": "c"},
		},
	}
	newGet := func(w http.ResponseWriter, _ *http.Request) {
		writeJSONBody(w, instanceJSON(t, "1", currentSpec))
	}

	// what the write sends
	var written map[string]any
	srv := newUpdateServer(t, newGet, func(w http.ResponseWriter, r *http.Request) {
		written = decodeSpec(t, r)
		writeJSONBody(w, instanceJSON(t, "2", written))
	})
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

	opts := baseUpdateOpts()
	opts.Set = []string{"version=null"}

	iu := NewUpdater(Config{}, zap.NewNop().Sugar())
	require.NoError(t, iu.Run(context.Background(), opts, cfgPath))

	// what the preview claims
	previewSrv := newUpdateServer(t, newGet, nil)
	defer previewSrv.Close()
	previewCfg := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(previewSrv.URL).Save(previewCfg))

	previewOpts := opts
	previewOpts.DryRun = true
	stdout := captureStdout(t, func() {
		require.NoError(t, iu.Run(context.Background(), previewOpts, previewCfg))
	})

	var previewed client.Instance
	require.NoError(t, json.Unmarshal([]byte(stdout), &previewed))
	previewSpec, err := specToMap(previewed.Spec)
	require.NoError(t, err)
	assert.Equal(t, written, previewSpec, "dry-run preview must match what the write sends")
}

// Nulling a required field must be refused by both write and --dry-run,
// without reaching the PUT.
func TestUpdate_NullOnRequiredFieldRejected(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"backup": map[string]any{
			"enabled":  true,
			"classRef": map[string]any{"name": "c"},
		},
	}

	for _, tc := range []struct {
		name   string
		dryRun bool
	}{
		{"write", false},
		{"dry run", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newUpdateServer(t,
				func(w http.ResponseWriter, _ *http.Request) {
					writeJSONBody(w, instanceJSON(t, "1", currentSpec))
				},
				nil, // must never reach the PUT
			)
			defer srv.Close()

			cfgPath := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

			opts := baseUpdateOpts()
			opts.Set = []string{"backup.enabled=null"}
			opts.DryRun = tc.dryRun

			iu := NewUpdater(Config{}, zap.NewNop().Sugar())
			err := iu.Run(context.Background(), opts, cfgPath)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cannot unset backup.enabled")
		})
	}
}

// A null on a required field inside a list element must be rejected too, not
// only at the top level: stripNulls has to walk lists, not just maps.
func TestUpdate_NullOnRequiredFieldInListRejected(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"backup": map[string]any{
			"enabled":  true,
			"classRef": map[string]any{"name": "c"},
			"storages": []any{map[string]any{
				"storageRef": map[string]any{"name": "s1"},
				"pitr":       map[string]any{"enabled": true},
			}},
		},
	}

	srv := newUpdateServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			writeJSONBody(w, instanceJSON(t, "1", currentSpec))
		},
		nil, // must never reach the PUT
	)
	defer srv.Close()

	valuesPath := filepath.Join(t.TempDir(), "values.yaml")
	require.NoError(t, os.WriteFile(valuesPath,
		[]byte("backup:\n  storages:\n    - storageRef:\n        name: s1\n      pitr:\n        enabled: null\n"), 0o600))

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

	opts := baseUpdateOpts()
	opts.ValuesFile = valuesPath

	iu := NewUpdater(Config{}, zap.NewNop().Sugar())
	err := iu.Run(context.Background(), opts, cfgPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backup.storages[0].pitr.enabled")
}

// A typo'd component name must be rejected, not PUT as a new component.
func TestUpdate_UnknownComponentRejected(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"topology":    map[string]any{"type": "replicaset"},
		"components":  map[string]any{"engine": map[string]any{"replicas": 3}},
	}

	srv := newUpdateServerWithProvider(t,
		func(w http.ResponseWriter, _ *http.Request) {
			writeJSONBody(w, instanceJSON(t, "1", currentSpec))
		},
		nil, // must never reach the PUT
	)
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

	opts := baseUpdateOpts()
	opts.Set = []string{"components.engien.replicas=5"}

	iu := NewUpdater(Config{}, zap.NewNop().Sugar())
	err := iu.Run(context.Background(), opts, cfgPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engien")
}

func TestUpdate_DryRun_NoWrite(t *testing.T) {
	t.Parallel()

	currentSpec := map[string]any{
		"providerRef": map[string]any{"name": "psmdb"},
		"components":  map[string]any{"engine": map[string]any{"replicas": 3}},
	}

	srv := newUpdateServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			writeJSONBody(w, instanceJSON(t, "1", currentSpec))
		},
		nil, // PUT must not be called
	)
	defer srv.Close()

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, newTestConfig(srv.URL).Save(cfgPath))

	opts := baseUpdateOpts()
	opts.Set = []string{"components.engine.replicas=5"}
	opts.DryRun = true

	iu := NewUpdater(Config{}, zap.NewNop().Sugar())
	err := iu.Run(context.Background(), opts, cfgPath)
	require.NoError(t, err)
}
