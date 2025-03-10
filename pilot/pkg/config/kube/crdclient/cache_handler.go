// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crdclient

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"

	//  import GKE cluster authentication plugin
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	//  import OIDC cluster authentication plugin, e.g. for Tectonic
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/client-go/tools/cache"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/config"
	"istio.io/pkg/log"
)

// cacheHandler abstracts the logic of an informer with a set of handlers. Handlers can be added at runtime
// and will be invoked on each informer event.
type cacheHandler struct {
	client   *Client
	informer cache.SharedIndexInformer
	// preferredGvk is the GVK we use internally. This is typically the same as clusterGvk, unless
	// we support multiple versions and the cluster we are connected to does not support or preferred version.
	// All calls to the client will come in as preferredGvk types.
	preferredGvk config.GroupVersionKind
	// clusterGvk is the actually GVK in the cluster. This may differ from preferredGvk when using multiple versions.
	// All reads and writes to the cluster will use this GVK.
	clusterGvk config.GroupVersionKind
	lister     func(namespace string) cache.GenericNamespaceLister
}

func (h *cacheHandler) onEvent(old interface{}, curr interface{}, event model.Event) error {
	if err := h.client.checkReadyForEvents(curr); err != nil {
		return err
	}

	currItem, ok := curr.(runtime.Object)
	if !ok {
		scope.Warnf("New Object can not be converted to runtime Object %v, is type %T", curr, curr)
		return nil
	}
	currConfig := TranslateObject(currItem, h.preferredGvk, h.client.domainSuffix)

	var oldConfig config.Config
	if old != nil {
		oldItem, ok := old.(runtime.Object)
		if !ok {
			log.Warnf("Old Object can not be converted to runtime Object %v, is type %T", old, old)
			return nil
		}
		oldConfig = TranslateObject(oldItem, h.preferredGvk, h.client.domainSuffix)
	}

	// TODO we may consider passing a pointer to handlers instead of the value. While spec is a pointer, the meta will be copied
	for _, f := range h.client.handlers[h.preferredGvk] {
		f(oldConfig, currConfig, event)
	}
	return nil
}

func createCacheHandler(cl *Client, i informers.GenericInformer, preferredGvk, clusterGvk config.GroupVersionKind, clusterScoped bool) *cacheHandler {
	scope.Debugf("registered CRD %v", preferredGvk)
	h := &cacheHandler{
		client:       cl,
		clusterGvk:   clusterGvk,
		preferredGvk: preferredGvk,
		informer:     i.Informer(),
	}
	if preferredGvk != clusterGvk {
		scope.Infof("preferred version %v is not available, reading %v", preferredGvk, clusterGvk)
	}
	h.lister = func(namespace string) cache.GenericNamespaceLister {
		if clusterScoped {
			return i.Lister()
		}
		return i.Lister().ByNamespace(namespace)
	}
	kind := preferredGvk.Kind
	i.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			incrementEvent(kind, "add")
			if !cl.beginSync.Load() {
				return
			}
			cl.queue.Push(func() error {
				return h.onEvent(nil, obj, model.EventAdd)
			})
		},
		UpdateFunc: func(old, cur interface{}) {
			incrementEvent(kind, "update")
			if !cl.beginSync.Load() {
				return
			}
			cl.queue.Push(func() error {
				return h.onEvent(old, cur, model.EventUpdate)
			})
		},
		DeleteFunc: func(obj interface{}) {
			incrementEvent(kind, "delete")
			if !cl.beginSync.Load() {
				return
			}
			cl.queue.Push(func() error {
				return h.onEvent(nil, obj, model.EventDelete)
			})
		},
	})
	return h
}
