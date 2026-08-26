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

// Package validation provides the validation handler.
package validation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	api "github.com/openeverest/openeverest/v2/internal/server/api"
	"github.com/openeverest/openeverest/v2/pkg/events"
)

// ListInstances proxies the request to the next handler.
func (h *validateHandler) ListInstances(ctx context.Context, cluster, namespace string) (*corev1alpha1.InstanceList, error) {
	return h.next.ListInstances(ctx, cluster, namespace)
}

// GetInstance proxies the request to the next handler.
func (h *validateHandler) GetInstance(ctx context.Context, cluster, namespace, name string) (*corev1alpha1.Instance, error) {
	return h.next.GetInstance(ctx, cluster, namespace, name)
}

// CreateInstance proxies the request to the next handler.
func (h *validateHandler) CreateInstance(ctx context.Context, cluster string, instance *corev1alpha1.Instance) (*corev1alpha1.Instance, error) {
	// Add validation here if needed in the future
	return h.next.CreateInstance(ctx, cluster, instance)
}

// UpdateInstance proxies the request to the next handler.
func (h *validateHandler) UpdateInstance(ctx context.Context, cluster string, instance *corev1alpha1.Instance) (*corev1alpha1.Instance, error) {
	// Add validation here if needed in the future
	return h.next.UpdateInstance(ctx, cluster, instance)
}

// PatchInstance rejects a patch naming a member the caller may not write, then proxies to the next handler.
// A merge patch can address any member of the stored object, not just the ones a client is meant to set.
func (h *validateHandler) PatchInstance(ctx context.Context, cluster, namespace, name string, patch []byte) (*corev1alpha1.Instance, error) {
	var doc map[string]any
	// Without the nil check a "null" body reaches the API server as an empty patch.
	if err := json.Unmarshal(patch, &doc); err != nil || doc == nil {
		return nil, errors.Join(ErrInvalidRequest, errors.New("patch must be a JSON object"))
	}
	// The CRD's status subresource already blocks this, so it is belt-and-braces.
	if _, found := doc["status"]; found {
		return nil, errors.Join(ErrInvalidRequest, errors.New("status may not be patched"))
	}
	if metadata, isObject := doc["metadata"].(map[string]any); isObject {
		for _, member := range []string{"ownerReferences", "finalizers", "name", "namespace"} {
			if _, found := metadata[member]; found {
				return nil, errors.Join(ErrInvalidRequest, fmt.Errorf("metadata.%s may not be patched", member))
			}
		}
		// The typed verbs stamp these after decoding, so a caller cannot author
		// them there. A merge patch is relayed as sent, so it could.
		if annotations, isObject := metadata["annotations"].(map[string]any); isObject {
			for _, key := range []string{events.AnnotationActorType, events.AnnotationActorID} {
				if _, found := annotations[key]; found {
					return nil, errors.Join(ErrInvalidRequest, fmt.Errorf("annotation %s may not be patched", key))
				}
			}
		}
	}
	return h.next.PatchInstance(ctx, cluster, namespace, name, patch)
}

// DeleteInstance proxies the request to the next handler.
func (h *validateHandler) DeleteInstance(ctx context.Context, cluster, namespace, name string, params *api.DeleteInstanceParams) error {
	return h.next.DeleteInstance(ctx, cluster, namespace, name, params)
}

// GetInstanceConnection proxies the request to the next handler.
func (h *validateHandler) GetInstanceConnection(ctx context.Context, cluster, namespace, name string) (*api.InstanceConnectionDetails, error) {
	return h.next.GetInstanceConnection(ctx, cluster, namespace, name)
}
