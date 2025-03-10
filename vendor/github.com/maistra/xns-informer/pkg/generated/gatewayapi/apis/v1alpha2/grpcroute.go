/*
Copyright Red Hat, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by xns-informer-gen. DO NOT EDIT.

package v1alpha2

import (
	"context"
	time "time"

	internalinterfaces "github.com/maistra/xns-informer/pkg/generated/gatewayapi/internalinterfaces"
	informers "github.com/maistra/xns-informer/pkg/informers"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
	apisv1alpha2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
	versioned "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"
	v1alpha2 "sigs.k8s.io/gateway-api/pkg/client/listers/apis/v1alpha2"
)

// GRPCRouteInformer provides access to a shared informer and lister for
// GRPCRoutes.
type GRPCRouteInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1alpha2.GRPCRouteLister
}

type gRPCRouteInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespaces       informers.NamespaceSet
}

// NewGRPCRouteInformer constructs a new informer for GRPCRoute type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewGRPCRouteInformer(client versioned.Interface, namespaces informers.NamespaceSet, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredGRPCRouteInformer(client, namespaces, resyncPeriod, indexers, nil)
}

// NewFilteredGRPCRouteInformer constructs a new informer for GRPCRoute type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredGRPCRouteInformer(client versioned.Interface, namespaces informers.NamespaceSet, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	newInformer := func(namespace string) cache.SharedIndexInformer {
		return cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
					if tweakListOptions != nil {
						tweakListOptions(&options)
					}
					return client.GatewayV1alpha2().GRPCRoutes(namespace).List(context.TODO(), options)
				},
				WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
					if tweakListOptions != nil {
						tweakListOptions(&options)
					}
					return client.GatewayV1alpha2().GRPCRoutes(namespace).Watch(context.TODO(), options)
				},
			},
			&apisv1alpha2.GRPCRoute{},
			resyncPeriod,
			indexers,
		)
	}

	return informers.NewMultiNamespaceInformer(namespaces, resyncPeriod, newInformer)
}

func (f *gRPCRouteInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredGRPCRouteInformer(client, f.namespaces, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *gRPCRouteInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&apisv1alpha2.GRPCRoute{}, f.defaultInformer)
}

func (f *gRPCRouteInformer) Lister() v1alpha2.GRPCRouteLister {
	return v1alpha2.NewGRPCRouteLister(f.Informer().GetIndexer())
}
