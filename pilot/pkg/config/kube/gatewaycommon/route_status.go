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

package gatewaycommon

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gw "sigs.k8s.io/gateway-api/apis/v1"

	"istio.io/api/label"
	"istio.io/istio/pilot/pkg/status"
	"istio.io/istio/pkg/config/schema/kubetypes"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/ptr"
	"istio.io/istio/pkg/revisions"
	"istio.io/istio/pkg/slices"
	"istio.io/istio/pkg/util/sets"
)

// RouteStatusOwners resolves ownership independently of listener validity and attachment permission.
type RouteStatusOwners struct {
	Gateways     krt.Collection[*gw.Gateway]
	ListenerSets krt.Collection[*gw.ListenerSet]
	Namespaces   krt.Collection[*corev1.Namespace]
	TagWatcher   krt.RecomputeProtected[revisions.TagWatcher]
	Controllers  sets.Set[gw.GatewayController]
}

func (o RouteStatusOwners) owns(ctx krt.HandlerContext, route controllers.Object, ref gw.ParentReference) bool {
	ns := string(ptr.OrDefault(ref.Namespace, gw.Namespace(route.GetNamespace())))
	group := ptr.OrDefault(ref.Group, gw.Group(gw.GroupName))
	kind := ptr.OrDefault(ref.Kind, gw.Kind("Gateway"))
	name := string(ref.Name)
	if group == gw.GroupName && kind == "ListenerSet" {
		ls := ptr.Flatten(krt.FetchOne(ctx, o.ListenerSets, krt.FilterKey(ns+"/"+name)))
		if ls != nil {
			ns = string(ptr.OrDefault(ls.Spec.ParentRef.Namespace, gw.Namespace(ls.Namespace)))
			name = string(ls.Spec.ParentRef.Name)
			kind = "Gateway"
		}
	}
	meta := metav1.ObjectMeta{Namespace: route.GetNamespace(), Labels: route.GetLabels()}
	if group == gw.GroupName && kind == "Gateway" {
		gateway := ptr.Flatten(krt.FetchOne(ctx, o.Gateways, krt.FilterKey(ns+"/"+name)))
		if gateway != nil {
			meta = gateway.ObjectMeta
		}
	}
	// IsMine reads namespaces internally; record the dependency so namespace relabeling reconciles status too.
	namespace := ptr.Flatten(krt.FetchOne(ctx, o.Namespaces, krt.FilterKey(meta.Namespace)))
	if _, explicit := meta.Labels[label.IoIstioRev.Name]; !explicit && namespace != nil {
		// Use the fetched namespace's label, avoiding a race with TagWatcher's separate informer.
		meta.Labels = map[string]string{label.IoIstioRev.Name: namespace.Labels[label.IoIstioRev.Name]}
	}
	return o.TagWatcher.Get(ctx).IsMine(meta)
}

type routeParentKey struct {
	controller gw.GatewayController
	group      gw.Group
	kind       gw.Kind
	namespace  gw.Namespace
	name       gw.ObjectName
	section    gw.SectionName
	port       gw.PortNumber
}

func parentStatusKey(parent gw.RouteParentStatus, namespace string) routeParentKey {
	ref := parent.ParentRef
	return routeParentKey{
		controller: parent.ControllerName,
		group:      ptr.OrDefault(ref.Group, gw.Group(gw.GroupName)),
		kind:       ptr.OrDefault(ref.Kind, gw.Kind("Gateway")),
		namespace:  ptr.OrDefault(ref.Namespace, gw.Namespace(namespace)),
		name:       ref.Name, section: ptr.OrEmpty(ref.SectionName), port: ptr.OrEmpty(ref.Port),
	}
}

// RouteStatusUpdate replaces only this revision's parent entries at write time.
type RouteStatusUpdate struct {
	Status    any
	owned     sets.Set[routeParentKey]
	namespace string
}

func (u RouteStatusUpdate) Equals(other RouteStatusUpdate) bool {
	return krt.Equal(u.Status, other.Status) && krt.Equal(u.owned, other.owned) &&
		u.namespace == other.namespace
}

// RegisterRouteStatus retains config visibility across revisions while restricting status writes to parent owners.
func RegisterRouteStatus[I controllers.Object, S any](
	queue *status.StatusCollections,
	collection krt.StatusCollection[I, S],
	getStatus func(I) S,
	owners RouteStatusOwners,
	opts krt.OptionsBuilder,
) {
	updates := krt.NewCollection(collection, func(ctx krt.HandlerContext, obj krt.ObjectWithStatus[I, S]) *krt.ObjectWithStatus[I, RouteStatusUpdate] {
		current := getStatus(obj.Obj)
		update := RouteStatusUpdate{
			Status: &obj.Status, owned: sets.New[routeParentKey](),
			namespace: obj.Obj.GetNamespace(),
		}
		// Include old status references so removed parents can be cleaned up, even after a restart.
		for _, parents := range [][]gw.RouteParentStatus{routeParents(&current), routeParents(update.Status)} {
			for _, parent := range parents {
				if owners.Controllers.Contains(parent.ControllerName) && owners.owns(ctx, obj.Obj, parent.ParentRef) {
					update.owned.Insert(parentStatusKey(parent, update.namespace))
				}
			}
		}
		return &krt.ObjectWithStatus[I, RouteStatusUpdate]{Obj: obj.Obj, Status: update}
	}, opts.WithName("RouteStatus/"+ptr.TypeName[I]())...)
	queue.Register(func(writer status.Queue) krt.HandlerRegistration {
		return updates.Register(func(event krt.Event[krt.ObjectWithStatus[I, RouteStatusUpdate]]) {
			if event.Event == controllers.EventDelete {
				return
			}
			obj := event.Latest()
			live := getStatus(obj.Obj)
			if krt.Equal(any(&live), MergeRouteStatus(&live, obj.Status)) {
				return
			}
			writer.EnqueueStatusUpdateResource(obj.Status, status.Resource{
				GroupVersionResource: kubetypes.GvrFromObject(obj.Obj),
				Namespace:            obj.Obj.GetNamespace(), Name: obj.Obj.GetName(),
				Generation: strconv.FormatInt(obj.Obj.GetGeneration(), 10),
			})
		})
	})
}

func routeParents(s any) []gw.RouteParentStatus {
	switch s := s.(type) {
	case *gw.HTTPRouteStatus:
		return s.Parents
	case *gw.GRPCRouteStatus:
		return s.Parents
	case *gw.TCPRouteStatus:
		return s.Parents
	case *gw.TLSRouteStatus:
		return s.Parents
	default:
		panic("unsupported route status")
	}
}

// MergeRouteStatus uses the status manager's latest snapshot. UpdateStatus carries its resourceVersion,
// so concurrent writers conflict instead of overwriting each other; the manager retries with a fresh snapshot.
func MergeRouteStatus(live, desired any) any {
	u, ok := desired.(RouteStatusUpdate)
	if !ok {
		return desired
	}
	if len(u.owned) == 0 {
		return live
	}
	incoming := slices.Filter(routeParents(u.Status), func(p gw.RouteParentStatus) bool {
		return u.owned.Contains(parentStatusKey(p, u.namespace))
	})
	merged := make([]gw.RouteParentStatus, 0, len(routeParents(live))+len(incoming))
	for _, p := range routeParents(live) {
		key := parentStatusKey(p, u.namespace)
		if !u.owned.Contains(key) {
			merged = append(merged, p)
			continue
		}
		if i := slices.IndexFunc(incoming, func(p gw.RouteParentStatus) bool {
			return parentStatusKey(p, u.namespace) == key
		}); i >= 0 {
			merged = append(merged, incoming[i])
			incoming = slices.Delete(incoming, i)
		}
	}
	merged = append(merged, incoming...)
	if len(merged) == 0 && len(routeParents(live)) == 0 {
		return live
	}
	switch s := live.(type) {
	case *gw.HTTPRouteStatus:
		out := s.DeepCopy()
		out.Parents = merged
		return out
	case *gw.GRPCRouteStatus:
		out := s.DeepCopy()
		out.Parents = merged
		return out
	case *gw.TCPRouteStatus:
		out := s.DeepCopy()
		out.Parents = merged
		return out
	case *gw.TLSRouteStatus:
		out := s.DeepCopy()
		out.Parents = merged
		return out
	default:
		panic("unsupported route status")
	}
}
