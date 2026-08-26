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

// Package client is the HTTP client for the OpenEverest API.
package client

import "context"

// PatchInstanceWithResponse applies an RFC 7386 merge patch to an instance,
// taking the same shape as every other patch operation's client method.
// oapi-codegen derives its own name from the media type, which is why the
// method this delegates to is called what it is.
func (c *ClientWithResponses) PatchInstanceWithResponse(ctx context.Context, cluster, namespace, instance string, body map[string]any, reqEditors ...RequestEditorFn) (*PatchInstanceResponse, error) {
	return c.PatchInstanceWithApplicationMergePatchPlusJSONBodyWithResponse(
		ctx, cluster, namespace, instance, body, reqEditors...,
	)
}
