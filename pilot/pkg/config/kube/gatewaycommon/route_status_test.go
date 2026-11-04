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
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gw "sigs.k8s.io/gateway-api/apis/v1"

	"istio.io/api/label"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/status"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/constants"
	"istio.io/istio/pkg/config/schema/gvk"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/ptr"
	"istio.io/istio/pkg/revisions"
	"istio.io/istio/pkg/test"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/util/sets"
)

func TestMergeRouteStatus(t *testing.T) {
	for _, controller := range []gw.GatewayController{
		"istio.io/gateway-controller", constants.ManagedAgentgatewayController, constants.ManagedAgentgatewayWaypointController,
	} {
		for name, wrap := range map[string]func([]gw.RouteParentStatus) any{
			"HTTP": func(p []gw.RouteParentStatus) any {
				return &gw.HTTPRouteStatus{RouteStatus: gw.RouteStatus{Parents: p}}
			},
			"GRPC": func(p []gw.RouteParentStatus) any {
				return &gw.GRPCRouteStatus{RouteStatus: gw.RouteStatus{Parents: p}}
			},
			"TCP": func(p []gw.RouteParentStatus) any { return &gw.TCPRouteStatus{RouteStatus: gw.RouteStatus{Parents: p}} },
			"TLS": func(p []gw.RouteParentStatus) any { return &gw.TLSRouteStatus{RouteStatus: gw.RouteStatus{Parents: p}} },
		} {
			t.Run(string(controller)+"/"+name, func(t *testing.T) {
				parent := func(name string, generation int64) gw.RouteParentStatus {
					return gw.RouteParentStatus{ParentRef: gw.ParentReference{Name: gw.ObjectName(name)}, ControllerName: controller,
						Conditions: []metav1.Condition{{Type: "Accepted", ObservedGeneration: generation,
							LastTransitionTime: metav1.NewTime(time.Unix(generation, 0))}}}
				}
				a, b, foreign := parent("a", 1), parent("b", 2), parent("a", 3)
				foreign.ControllerName = "example.com/other"
				live := wrap([]gw.RouteParentStatus{a, b, foreign})
				a2 := parent("a", 4)
				a2.ParentRef.Group = ptr.Of(gw.Group(gw.GroupName))
				a2.ParentRef.Kind = ptr.Of(gw.Kind("Gateway"))
				a2.ParentRef.Namespace = ptr.Of(gw.Namespace("ns"))
				update := RouteStatusUpdate{Status: wrap([]gw.RouteParentStatus{a2, parent("b", 1), foreign}),
					owned: sets.New(parentStatusKey(a, "ns")), namespace: "ns"}
				got := MergeRouteStatus(live, update)
				assert.Equal(t, routeParents(got), []gw.RouteParentStatus{a2, b, foreign})
				assert.Equal(t, routeParents(live), []gw.RouteParentStatus{a, b, foreign})
				// The second revision's stale snapshot must not revert the first revision's write.
				b2 := parent("b", 5)
				other := RouteStatusUpdate{Status: wrap([]gw.RouteParentStatus{a, b2}),
					owned: sets.New(parentStatusKey(b, "ns")), namespace: "ns"}
				got = MergeRouteStatus(got, other)
				assert.Equal(t, routeParents(got), []gw.RouteParentStatus{a2, b2, foreign})
				assert.Equal(t, MergeRouteStatus(got, update), got)
				update.Status = wrap(nil)
				got = MergeRouteStatus(got, update)
				assert.Equal(t, routeParents(got), []gw.RouteParentStatus{b2, foreign})
				// A different listener/port is a different parent entry.
				section := a
				section.ParentRef.SectionName = ptr.Of(gw.SectionName("http"))
				port := a
				port.ParentRef.Port = ptr.Of(gw.PortNumber(80))
				assert.Equal(t, parentStatusKey(section, "ns") == parentStatusKey(a, "ns"), false)
				assert.Equal(t, parentStatusKey(port, "ns") == parentStatusKey(a, "ns"), false)
			})
		}
	}
}

type routeStatusTagWatcher struct {
	revisions.TagWatcher
	tagOwned atomic.Bool
	revision string
}

func (w *routeStatusTagWatcher) IsMine(meta metav1.ObjectMeta) bool {
	rev := meta.Labels[label.IoIstioRev.Name]
	ownedRevision := w.revision
	if ownedRevision == "" {
		ownedRevision = "blue"
	}
	return rev == ownedRevision || rev == "tag" && w.tagOwned.Load()
}

type routeStatusQueue struct {
	mu      sync.Mutex
	updates []RouteStatusUpdate
}

func (q *routeStatusQueue) EnqueueStatusUpdateResource(context any, _ status.Resource) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.updates = append(q.updates, context.(RouteStatusUpdate))
}

func (q *routeStatusQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.updates)
}

func TestRegisterRouteStatusOwnership(t *testing.T) {
	for _, controller := range []gw.GatewayController{
		"istio.io/gateway-controller", constants.ManagedAgentgatewayController, constants.ManagedAgentgatewayWaypointController,
	} {
		t.Run(string(controller), func(t *testing.T) {
			stop := test.NewStop(t)
			opts := krt.NewOptionsBuilder(stop, "test", nil)
			watcher := &routeStatusTagWatcher{}
			tags := krt.NewRecomputeProtected[revisions.TagWatcher](watcher, true, opts.WithName("tags")...)
			gateways := krt.NewMutableCollection[*gw.Gateway](nil, nil, opts.WithName("gateways")...)
			listenerSets := krt.NewMutableCollection[*gw.ListenerSet](nil, nil, opts.WithName("sets")...)
			namespaces := krt.NewMutableCollection[*corev1.Namespace](nil, nil, opts.WithName("namespaces")...)
			owners := RouteStatusOwners{Gateways: gateways.AsCollection(), ListenerSets: listenerSets.AsCollection(),
				Namespaces: namespaces.AsCollection(), TagWatcher: tags, Controllers: sets.New(controller)}
			parent := gw.RouteParentStatus{ParentRef: gw.ParentReference{Name: "gateway"}, ControllerName: controller,
				Conditions: []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionTrue}}}
			route := &gw.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: "route", Namespace: "ns", Generation: 1}}
			type entry = krt.ObjectWithStatus[*gw.HTTPRoute, gw.HTTPRouteStatus]
			current := entry{Obj: route, Status: gw.HTTPRouteStatus{RouteStatus: gw.RouteStatus{Parents: []gw.RouteParentStatus{parent}}}}
			col := krt.NewMutableCollection[entry](nil, []entry{current}, opts.WithName("routes")...)
			queue := &routeStatusQueue{}
			statuses := &status.StatusCollections{}
			RegisterRouteStatus(statuses, col.AsCollection(), func(r *gw.HTTPRoute) gw.HTTPRouteStatus { return r.Status }, owners, opts)
			for _, syncer := range statuses.SetQueue(queue) {
				syncer.WaitUntilSynced(stop)
			}
			assert.Equal(t, queue.count(), 0)
			// CIO: only the Gateway has a revision label; no listeners are needed for ownership.
			gateway := &gw.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "ns", Labels: map[string]string{label.IoIstioRev.Name: "blue"}}}
			gateways.UpdateObject(gateway)
			assert.EventuallyEqual(t, queue.count, 1)
			// A conflicting Route label cannot override the Gateway's owner.
			current.Obj = route.DeepCopy()
			current.Obj.Labels = map[string]string{label.IoIstioRev.Name: "red"}
			col.UpdateObject(current)
			assert.EventuallyEqual(t, queue.count, 2)
			// Feeding back the written status must suppress further writes.
			current.Obj = current.Obj.DeepCopy()
			current.Obj.Status = current.Status
			col.UpdateObject(current)
			col.AsCollection().WaitUntilSynced(stop)
			// Remove a parent: its current Gateway owner is responsible for cleanup.
			current.Status.Parents = nil
			col.UpdateObject(current)
			assert.EventuallyEqual(t, queue.count, 3)
			queue.mu.Lock()
			cleanup := queue.updates[len(queue.updates)-1]
			queue.mu.Unlock()
			assert.Equal(t, len(routeParents(MergeRouteStatus(&current.Obj.Status, cleanup))), 0)
		})
	}
}

func TestAgentgatewayServiceParentStatus(t *testing.T) {
	stop := test.NewStop(t)
	opts := krt.NewOptionsBuilder(stop, "service", nil)
	owners := RouteStatusOwners{
		Gateways:     krt.NewStaticCollection[*gw.Gateway](nil, nil, opts.WithName("gateways")...),
		ListenerSets: krt.NewStaticCollection[*gw.ListenerSet](nil, nil, opts.WithName("sets")...),
		Namespaces:   krt.NewStaticCollection[*corev1.Namespace](nil, nil, opts.WithName("namespaces")...),
		TagWatcher:   krt.NewRecomputeProtected[revisions.TagWatcher](&routeStatusTagWatcher{}, true, opts.WithName("tags")...),
		Controllers:  sets.New[gw.GatewayController](constants.ManagedAgentgatewayController, constants.ManagedAgentgatewayWaypointController),
	}
	parent := gw.RouteParentStatus{
		ParentRef:      gw.ParentReference{Group: ptr.Of(gw.Group("")), Kind: ptr.Of(gw.Kind("Service")), Name: "service"},
		ControllerName: constants.ManagedAgentgatewayWaypointController,
		Conditions:     []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionTrue, ObservedGeneration: 2}},
	}
	// The same parent reference under different controllers must remain distinct.
	gateway, foreign := parent, parent
	gateway.ControllerName = constants.ManagedAgentgatewayController
	foreign.ControllerName = "example.com/other"
	old := parent
	old.Conditions = []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionTrue, ObservedGeneration: 1}}
	route := &gw.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: "route", Namespace: "ns", Generation: 2,
		Labels: map[string]string{label.IoIstioRev.Name: "red"}},
		Status: gw.HTTPRouteStatus{RouteStatus: gw.RouteStatus{Parents: []gw.RouteParentStatus{old, gateway, foreign}}}}
	type entry = krt.ObjectWithStatus[*gw.HTTPRoute, gw.HTTPRouteStatus]
	current := entry{Obj: route, Status: gw.HTTPRouteStatus{RouteStatus: gw.RouteStatus{Parents: []gw.RouteParentStatus{parent, gateway, foreign}}}}
	col := krt.NewMutableCollection(nil, []entry{current}, opts.WithName("routes")...)
	queue := &routeStatusQueue{}
	statuses := &status.StatusCollections{}
	RegisterRouteStatus(statuses, col.AsCollection(), func(r *gw.HTTPRoute) gw.HTTPRouteStatus { return r.Status }, owners, opts)
	for _, syncer := range statuses.SetQueue(queue) {
		syncer.WaitUntilSynced(stop)
	}
	assert.Equal(t, queue.count(), 0)
	// Service parents retain the Route's revision ownership.
	current.Obj = route.DeepCopy()
	current.Obj.Labels[label.IoIstioRev.Name] = "blue"
	col.UpdateObject(current)
	assert.EventuallyEqual(t, queue.count, 1)
	queue.mu.Lock()
	update := queue.updates[0]
	queue.mu.Unlock()
	assert.Equal(t, routeParents(MergeRouteStatus(&route.Status, update)), []gw.RouteParentStatus{parent, gateway, foreign})
	// Removing the waypoint entry must preserve the other controller's entry at the same parent.
	current.Obj = current.Obj.DeepCopy()
	current.Obj.Status = current.Status
	current.Status.Parents = []gw.RouteParentStatus{gateway, foreign}
	col.UpdateObject(current)
	assert.EventuallyEqual(t, queue.count, 2)
	queue.mu.Lock()
	cleanup := queue.updates[1]
	queue.mu.Unlock()
	assert.Equal(t, routeParents(MergeRouteStatus(&current.Obj.Status, cleanup)), []gw.RouteParentStatus{gateway, foreign})
}

func TestRouteStatusOwnershipChanges(t *testing.T) {
	stop := test.NewStop(t)
	opts := krt.NewOptionsBuilder(stop, "owners", nil)
	watcher := &routeStatusTagWatcher{}
	tags := krt.NewRecomputeProtected[revisions.TagWatcher](watcher, true, opts.WithName("tags")...)
	gateways := krt.NewMutableCollection[*gw.Gateway](nil, nil, opts.WithName("gateways")...)
	listenerSets := krt.NewMutableCollection[*gw.ListenerSet](nil, nil, opts.WithName("sets")...)
	namespaces := krt.NewMutableCollection[*corev1.Namespace](nil, nil, opts.WithName("namespaces")...)
	owners := RouteStatusOwners{Gateways: gateways.AsCollection(), ListenerSets: listenerSets.AsCollection(),
		Namespaces: namespaces.AsCollection(), TagWatcher: tags}
	route := &gw.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: "route", Namespace: "ns",
		Labels: map[string]string{label.IoIstioRev.Name: "red"}},
		Spec: gw.HTTPRouteSpec{CommonRouteSpec: gw.CommonRouteSpec{ParentRefs: []gw.ParentReference{{Name: "gateway"}}}}}
	routes := krt.NewMutableCollection(nil, []*gw.HTTPRoute{route}, opts.WithName("routes")...)
	result := krt.NewCollection(routes.AsCollection(), func(ctx krt.HandlerContext, r *gw.HTTPRoute) *krt.ObjectWithStatus[*gw.HTTPRoute, bool] {
		return &krt.ObjectWithStatus[*gw.HTTPRoute, bool]{Obj: r, Status: owners.owns(ctx, r, r.Spec.ParentRefs[0])}
	}, opts.WithName("ownership")...)
	check := func(want bool) {
		t.Helper()
		assert.EventuallyEqual(t, func() bool {
			values := result.List()
			return len(values) == 1 && values[0].Status == want
		}, true)
	}
	check(false)
	gateway := &gw.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "ns",
		Labels: map[string]string{label.IoIstioRev.Name: "blue"}}}
	gateways.UpdateObject(gateway)
	check(true)
	gateway = gateway.DeepCopy()
	gateway.Labels[label.IoIstioRev.Name] = "red"
	gateways.UpdateObject(gateway)
	check(false)
	gateway = gateway.DeepCopy()
	gateway.Labels = nil
	gateways.UpdateObject(gateway)
	namespaces.UpdateObject(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns",
		Labels: map[string]string{label.IoIstioRev.Name: "blue"}}})
	check(true)
	namespaces.UpdateObject(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns",
		Labels: map[string]string{label.IoIstioRev.Name: "red"}}})
	check(false)
	gateway = gateway.DeepCopy()
	gateway.Labels = map[string]string{label.IoIstioRev.Name: "tag"}
	gateways.UpdateObject(gateway)
	watcher.tagOwned.Store(true)
	tags.TriggerRecomputation()
	check(true)
	watcher.tagOwned.Store(false)
	tags.TriggerRecomputation()
	check(false)
	watcher.tagOwned.Store(true)
	tags.TriggerRecomputation()
	check(true)
	// A cross-namespace ListenerSet follows its Gateway, not its own revision.
	listenerSets.UpdateObject(&gw.ListenerSet{ObjectMeta: metav1.ObjectMeta{Name: "set", Namespace: "other",
		Labels: map[string]string{label.IoIstioRev.Name: "red"}},
		Spec: gw.ListenerSetSpec{ParentRef: gw.ParentGatewayReference{Name: "gateway", Namespace: ptr.Of(gw.Namespace("ns"))}}})
	route = route.DeepCopy()
	route.Spec.ParentRefs = []gw.ParentReference{{Kind: ptr.Of(gw.Kind("ListenerSet")), Name: "set", Namespace: ptr.Of(gw.Namespace("other"))}}
	routes.UpdateObject(route)
	check(true)
	gateways.DeleteObject("ns/gateway")
	check(false)
	route = route.DeepCopy()
	route.Labels[label.IoIstioRev.Name] = "blue"
	routes.UpdateObject(route)
	check(true)
	listenerSets.DeleteObject("other/set")
	check(true)
	// Non-Gateway parents retain Route ownership.
	route = route.DeepCopy()
	route.Spec.ParentRefs = []gw.ParentReference{{Group: ptr.Of(gw.Group("")), Kind: ptr.Of(gw.Kind("Service")), Name: "service"}}
	routes.UpdateObject(route)
	check(true)
	route = route.DeepCopy()
	route.Labels[label.IoIstioRev.Name] = "red"
	routes.UpdateObject(route)
	check(false)
}

// conflictRouteStore models API-server resourceVersion checks and informer delivery.
type conflictRouteStore struct {
	model.ConfigStore
	mu        sync.Mutex
	current   config.Config
	reads     int
	conflicts int
	ready     chan struct{}
	stop      <-chan struct{}
	notify    func(config.Config)
}

func (s *conflictRouteStore) Get(config.GroupVersionKind, string, string) *config.Config {
	s.mu.Lock()
	out := s.current.DeepCopy()
	s.reads++
	reads := s.reads
	if reads == 2 {
		close(s.ready)
	}
	s.mu.Unlock()
	// Force the first two writers to read the same version before either writes.
	if reads <= 2 {
		select {
		case <-s.ready:
		case <-s.stop:
		}
	}
	return &out
}

func (s *conflictRouteStore) UpdateStatus(cfg config.Config) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cfg.ResourceVersion != s.current.ResourceVersion {
		s.conflicts++
		return "", kerrors.NewConflict(gw.Resource("httproutes"), cfg.Name, fmt.Errorf("stale resourceVersion"))
	}
	version, _ := strconv.Atoi(cfg.ResourceVersion)
	cfg.ResourceVersion = strconv.Itoa(version + 1)
	s.current = cfg.DeepCopy()
	s.notify(s.current)
	return cfg.ResourceVersion, nil
}

func TestRouteStatusConflictConverges(t *testing.T) {
	stop := test.NewStop(t)
	opts := krt.NewOptionsBuilder(stop, "conflict", nil)
	controller := gw.GatewayController("istio.io/gateway-controller")
	var gateways []*gw.Gateway
	var parents []gw.RouteParentStatus
	for _, rev := range []string{"blue", "red"} {
		gateways = append(gateways, &gw.Gateway{ObjectMeta: metav1.ObjectMeta{Name: rev, Namespace: "ns",
			Labels: map[string]string{label.IoIstioRev.Name: rev}}})
		parents = append(parents, gw.RouteParentStatus{ParentRef: gw.ParentReference{Name: gw.ObjectName(rev)}, ControllerName: controller,
			Conditions: []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionTrue, ObservedGeneration: 1}}})
	}
	type entry = krt.ObjectWithStatus[*gw.HTTPRoute, gw.HTTPRouteStatus]
	var inputs []krt.StaticCollection[entry]
	store := &conflictRouteStore{ready: make(chan struct{}), stop: stop, current: config.Config{
		Meta:   config.Meta{GroupVersionKind: gvk.HTTPRoute, Name: "route", Namespace: "ns", Generation: 1, ResourceVersion: "1"},
		Status: &gw.HTTPRouteStatus{},
	}}
	makeEntry := func(cfg config.Config) entry {
		return entry{Obj: &gw.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: cfg.Name, Namespace: cfg.Namespace,
			Generation: cfg.Generation, ResourceVersion: cfg.ResourceVersion}, Status: *cfg.Status.(*gw.HTTPRouteStatus).DeepCopy()},
			Status: gw.HTTPRouteStatus{RouteStatus: gw.RouteStatus{Parents: parents}}}
	}
	for range 2 {
		inputs = append(inputs, krt.NewMutableCollection(nil, []entry{makeEntry(store.current)}, opts.WithName(fmt.Sprintf("input-%d", len(inputs)))...))
	}
	store.notify = func(cfg config.Config) {
		for _, input := range inputs {
			input.UpdateObject(makeEntry(cfg))
		}
	}
	for i, rev := range []string{"blue", "red"} {
		owners := RouteStatusOwners{
			Gateways:     krt.NewStaticCollection(nil, gateways, opts.WithName(rev+"/gateways")...),
			ListenerSets: krt.NewStaticCollection[*gw.ListenerSet](nil, nil, opts.WithName(rev+"/sets")...),
			Namespaces:   krt.NewStaticCollection[*corev1.Namespace](nil, nil, opts.WithName(rev+"/namespaces")...),
			TagWatcher:   krt.NewRecomputeProtected[revisions.TagWatcher](&routeStatusTagWatcher{revision: rev}, true, opts.WithName(rev+"/tags")...),
			Controllers:  sets.New(controller),
		}
		registrations := &status.StatusCollections{}
		RegisterRouteStatus(registrations, inputs[i].AsCollection(), func(r *gw.HTTPRoute) gw.HTTPRouteStatus { return r.Status }, owners, opts)
		manager := status.NewManager(store)
		manager.Start(stop)
		registrations.SetQueue(manager.CreateGenericController(func(s status.Manipulator, desired any) {
			s.SetInner(MergeRouteStatus(s.Unwrap(), desired))
		}))
	}
	assert.EventuallyEqual(t, func() int {
		store.mu.Lock()
		defer store.mu.Unlock()
		return len(store.current.Status.(*gw.HTTPRouteStatus).Parents)
	}, 2)
	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, store.conflicts > 0, true)
	assert.Equal(t, store.current.Status.(*gw.HTTPRouteStatus).Parents[0].Conditions[0].ObservedGeneration, int64(1))
	assert.Equal(t, store.current.Status.(*gw.HTTPRouteStatus).Parents[1].Conditions[0].ObservedGeneration, int64(1))
}

// delayedRouteStore holds the writer's cache behind the Route collection until its first conflict.
type delayedRouteStore struct {
	*conflictRouteStore
	cached config.Config
}

func (s *delayedRouteStore) Get(config.GroupVersionKind, string, string) *config.Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.cached.DeepCopy()
	return &out
}

func (s *delayedRouteStore) UpdateStatus(cfg config.Config) (string, error) {
	version, err := s.conflictRouteStore.UpdateStatus(cfg)
	if kerrors.IsConflict(err) {
		s.mu.Lock()
		s.cached = s.current.DeepCopy()
		s.mu.Unlock()
	}
	return version, err
}

func TestRouteStatusConflictWithDelayedWriterCache(t *testing.T) {
	stop := test.NewStop(t)
	opts := krt.NewOptionsBuilder(stop, "delayed-cache", nil)
	controller := gw.GatewayController("istio.io/gateway-controller")
	var gateways []*gw.Gateway
	var parents []gw.RouteParentStatus
	for _, rev := range []string{"red", "blue"} {
		gateways = append(gateways, &gw.Gateway{ObjectMeta: metav1.ObjectMeta{Name: rev, Namespace: "ns",
			Labels: map[string]string{label.IoIstioRev.Name: rev}}})
		parents = append(parents, gw.RouteParentStatus{ParentRef: gw.ParentReference{Name: gw.ObjectName(rev)}, ControllerName: controller,
			Conditions: []metav1.Condition{{Type: "Accepted", Status: metav1.ConditionTrue, ObservedGeneration: 1}}})
	}
	stale := config.Config{
		Meta:   config.Meta{GroupVersionKind: gvk.HTTPRoute, Name: "route", Namespace: "ns", Generation: 1, ResourceVersion: "1"},
		Status: &gw.HTTPRouteStatus{},
	}
	live := stale.DeepCopy()
	live.ResourceVersion = "2"
	live.Status = &gw.HTTPRouteStatus{RouteStatus: gw.RouteStatus{Parents: parents[:1]}}
	store := &delayedRouteStore{
		conflictRouteStore: &conflictRouteStore{current: live, notify: func(config.Config) {}},
		cached:             stale,
	}
	owners := RouteStatusOwners{
		Gateways:     krt.NewStaticCollection(nil, gateways, opts.WithName("gateways")...),
		ListenerSets: krt.NewStaticCollection[*gw.ListenerSet](nil, nil, opts.WithName("sets")...),
		Namespaces:   krt.NewStaticCollection[*corev1.Namespace](nil, nil, opts.WithName("namespaces")...),
		TagWatcher:   krt.NewRecomputeProtected[revisions.TagWatcher](&routeStatusTagWatcher{revision: "blue"}, true, opts.WithName("tags")...),
		Controllers:  sets.New(controller),
	}
	type entry = krt.ObjectWithStatus[*gw.HTTPRoute, gw.HTTPRouteStatus]
	// The Route collection has already received revision red's write. There are no subsequent input events or edits.
	input := krt.NewStaticCollection(nil, []entry{{
		Obj: &gw.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: live.Name, Namespace: live.Namespace,
			Generation: live.Generation, ResourceVersion: live.ResourceVersion}, Status: *live.Status.(*gw.HTTPRouteStatus).DeepCopy()},
		Status: gw.HTTPRouteStatus{RouteStatus: gw.RouteStatus{Parents: parents}},
	}}, opts.WithName("routes")...)
	registrations := &status.StatusCollections{}
	RegisterRouteStatus(registrations, input, func(r *gw.HTTPRoute) gw.HTTPRouteStatus { return r.Status }, owners, opts)
	manager := status.NewManager(store)
	manager.Start(stop)
	registrations.SetQueue(manager.CreateGenericController(func(s status.Manipulator, desired any) {
		s.SetInner(MergeRouteStatus(s.Unwrap(), desired))
	}))
	assert.EventuallyEqual(t, func() []gw.RouteParentStatus {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.current.Status.(*gw.HTTPRouteStatus).DeepCopy().Parents
	}, parents)
	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, store.conflicts, 1)
	assert.Equal(t, store.current.ResourceVersion, "3")
	assert.Equal(t, store.current.Generation, int64(1))
}
