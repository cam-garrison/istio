//go:build integ

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

package revisions

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberetry "k8s.io/client-go/util/retry"
	gw "sigs.k8s.io/gateway-api/apis/v1"

	"istio.io/istio/pkg/config/constants"
	"istio.io/istio/pkg/config/protocol"
	"istio.io/istio/pkg/test/framework"
	"istio.io/istio/pkg/test/framework/components/crd"
	"istio.io/istio/pkg/test/framework/components/echo"
	"istio.io/istio/pkg/test/framework/components/echo/check"
	"istio.io/istio/pkg/test/framework/components/echo/deployment"
	"istio.io/istio/pkg/test/framework/components/echo/echotest"
	"istio.io/istio/pkg/test/framework/components/echo/match"
	"istio.io/istio/pkg/test/framework/components/istio"
	"istio.io/istio/pkg/test/framework/components/namespace"
	"istio.io/istio/pkg/test/framework/label"
	"istio.io/istio/pkg/test/framework/resource"
	"istio.io/istio/pkg/test/util/retry"
)

// TestMain defines the entrypoint for pilot tests using a standard Istio installation.
// If a test requires a custom install it should go into its own package, otherwise it should go
// here to reuse a single install across tests.
func TestMain(m *testing.M) {
	// nolint: staticcheck
	framework.
		NewSuite(m).
		RequireMultiPrimary().
		Label(label.Full).
		// Requires two CPs with specific names to be configured.
		Label(label.CustomSetup).
		Setup(istio.Setup(nil, func(_ resource.Context, cfg *istio.Config) {
			cfg.ControlPlaneValues = `
revision: stable
`
		})).
		Setup(istio.Setup(nil, func(_ resource.Context, cfg *istio.Config) {
			cfg.ControlPlaneValues = `
profile: empty
revision: canary
components:
  pilot:
    enabled: true
`
		})).
		Run()
}

// TestMultiRevision Sets up a simple client -> server call, where the client and server
// belong to different control planes.
func TestMultiRevision(t *testing.T) {
	framework.NewTest(t).
		Run(func(t framework.TestContext) {
			stable := namespace.NewOrFail(t, namespace.Config{
				Prefix:   "stable",
				Inject:   true,
				Revision: "stable",
			})
			canary := namespace.NewOrFail(t, namespace.Config{
				Prefix:   "canary",
				Inject:   true,
				Revision: "canary",
			})

			echos := deployment.New(t).
				WithClusters(t.Clusters()...).
				WithConfig(echo.Config{
					Service:   "client",
					Namespace: stable,
					Ports:     []echo.Port{},
				}).
				WithConfig(echo.Config{
					Service:   "server",
					Namespace: canary,
					Ports: []echo.Port{
						{
							Name:         "http",
							Protocol:     protocol.HTTP,
							WorkloadPort: 8090,
						},
					},
				}).
				WithConfig(echo.Config{
					Service:    "vm",
					Namespace:  canary,
					DeployAsVM: true,
					Ports:      []echo.Port{},
				}).
				BuildOrFail(t)

			echotest.New(t, echos).
				ConditionallyTo(echotest.ReachableDestinations).
				ToMatch(match.ServiceName(echo.NamespacedName{Name: "server", Namespace: canary})).
				Run(func(t framework.TestContext, from echo.Instance, to echo.Target) {
					retry.UntilSuccessOrFail(t, func() error {
						result, err := from.Call(echo.CallOptions{
							To: to,
							Port: echo.Port{
								Name: "http",
							},
							Retry: echo.Retry{
								NoRetry: true,
							},
							Check: check.And(
								check.OK(),
								check.ReachedTargetClusters(t),
							),
						})
						return check.And(
							check.NoError(),
							check.OK()).Check(result, err)
					}, retry.Delay(time.Millisecond*100))
				})
		})
}

func TestMultiRevisionRouteStatusHandling(t *testing.T) {
	framework.NewTest(t).
		Run(func(t framework.TestContext) {
			crd.DeployGatewayAPIOrSkip(t)
			cfg := istio.DefaultConfigOrFail(t, t)
			stable := namespace.NewOrFail(t, namespace.Config{
				Prefix:   "stable",
				Inject:   true,
				Revision: "stable",
			})
			canary := namespace.NewOrFail(t, namespace.Config{
				Prefix:   "canary",
				Inject:   true,
				Revision: "canary",
			})

			_ = deployment.New(t).
				WithClusters(t.Clusters()...).
				WithConfig(echo.Config{
					Service:   "client",
					Namespace: stable,
					Ports:     []echo.Port{},
				}).
				WithConfig(echo.Config{
					Service:   "server",
					Namespace: canary,
					Ports: []echo.Port{
						{
							Name:         "http",
							Protocol:     protocol.HTTP,
							WorkloadPort: 8090,
						},
					},
				}).
				BuildOrFail(t)

			t.ConfigIstio().Eval(canary.Name(), map[string]string{
				"namespace":    canary.Name(),
				"gatewayClass": cfg.GatewayClassName,
			}, `
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: test-gateway
  namespace: {{.namespace}}
spec:
  gatewayClassName: {{.gatewayClass}}
  listeners:
  - name: http
    hostname: "test.example.com"
    port: 80
    protocol: HTTP
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: test-route
  namespace: {{.namespace}}
spec:
  parentRefs:
  - name: test-gateway
  hostnames:
  - "test.example.com"
  rules:
  - matches:
    - path:
        type: PathPrefix
        value: /
    backendRefs:
    - name: server
      port: 8090
`).ApplyOrFail(t)

			getRoute := func() (int, error) {
				route, err := t.Clusters().Default().GatewayAPI().GatewayV1beta1().HTTPRoutes(canary.Name()).Get(context.Background(), "test-route", v1.GetOptions{})
				if err != nil {
					return 0, err
				}
				return len(route.Status.Parents), nil
			}

			// Wait for the status handler to populate parent status.
			retry.UntilSuccessOrFail(t, func() error {
				parentCount, err := getRoute()
				if err != nil {
					return err
				}
				if parentCount == 0 {
					return fmt.Errorf("waiting for httproute status parents")
				}
				return nil
			}, retry.Timeout(30*time.Second), retry.Delay(100*time.Millisecond))

			// Verify parent status is not cleared after it appears.
			retry.UntilSuccessOrFail(t, func() error {
				parentCount, err := getRoute()
				if err != nil {
					return err
				}
				if parentCount == 0 {
					return fmt.Errorf("httproute status was incorrectly overwritten")
				}
				return nil
			}, retry.Converge(10), retry.Delay(10*time.Millisecond))
		})
}

func TestRouteStatusWithParentsInDifferentRevisions(t *testing.T) {
	framework.NewTest(t).Run(func(t framework.TestContext) {
		crd.DeployGatewayAPIOrSkip(t)
		cfg := istio.DefaultConfigOrFail(t, t)
		controller := gw.GatewayController("istio.io/gateway-controller")
		listenerProtocol, port := "HTTP", "80"
		parentKinds := []string{"Gateway"}
		switch cfg.GatewayClassName {
		case constants.AgentgatewayClassName:
			controller = constants.ManagedAgentgatewayController
		case constants.AgentgatewayWaypointClassName:
			controller = constants.ManagedAgentgatewayWaypointController
			listenerProtocol, port = "HBONE", "15008"
			parentKinds = append(parentKinds, "Service")
		}
		for _, parentKind := range parentKinds {
			t.NewSubTest(parentKind).Run(func(t framework.TestContext) {
				ns := namespace.NewOrFail(t, namespace.Config{Prefix: "route-status", Inject: false})
				for _, revision := range []string{"stable", "canary"} {
					t.ConfigIstio().Eval(ns.Name(), map[string]string{
						"revision": revision, "class": cfg.GatewayClassName, "protocol": listenerProtocol, "port": port,
					}, `
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: {{.revision}}
  labels:
    istio.io/rev: {{.revision}}
    istio.io/waypoint-for: service
  annotations:
    networking.istio.io/service-type: ClusterIP
spec:
  gatewayClassName: {{.class}}
  listeners:
  - name: http
    port: {{.port}}
    protocol: {{.protocol}}
`).ApplyOrFail(t)
					if parentKind == "Service" {
						t.ConfigIstio().Eval(ns.Name(), map[string]string{"revision": revision}, `
apiVersion: v1
kind: Service
metadata:
  name: {{.revision}}-service
  labels:
    istio.io/use-waypoint: {{.revision}}
spec:
  ports:
  - name: http
    port: 80
`).ApplyOrFail(t)
					}
				}
				parentGroup := gw.Group(gw.GroupName)
				stableParent, canaryParent := gw.ObjectName("stable"), gw.ObjectName("canary")
				if parentKind == "Service" {
					parentGroup = ""
					stableParent, canaryParent = "stable-service", "canary-service"
				}
				t.ConfigIstio().Eval(ns.Name(), map[string]string{
					"kind": parentKind, "group": string(parentGroup), "stable": string(stableParent), "canary": string(canaryParent),
				}, `
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: shared
{{if eq .kind "Service"}}
  labels:
    istio.io/rev: stable
{{end}}
spec:
  parentRefs:
  - name: {{.stable}}
    kind: {{.kind}}
    group: "{{.group}}"
  - name: {{.canary}}
    kind: {{.kind}}
    group: "{{.group}}"
  rules:
  - filters:
    - type: RequestRedirect
      requestRedirect:
        hostname: example.com
`).ApplyOrFail(t)
				client := t.Clusters().Default().GatewayAPI().GatewayV1()
				routes := client.HTTPRoutes(ns.Name())
				check := func(names ...gw.ObjectName) {
					t.Helper()
					var version string
					retry.UntilSuccessOrFail(t, func() error {
						route, err := routes.Get(context.Background(), "shared", v1.GetOptions{})
						if err != nil {
							return err
						}
						if len(route.Status.Parents) != len(names) {
							return fmt.Errorf("got parents %v, want %v", route.Status.Parents, names)
						}
						for _, name := range names {
							found := false
							for _, parent := range route.Status.Parents {
								if parent.ParentRef.Name != name || parent.ControllerName != controller {
									continue
								}
								found = true
								for _, typ := range []string{"Accepted", "ResolvedRefs"} {
									valid := false
									for _, condition := range parent.Conditions {
										if condition.Type == typ && condition.Status == v1.ConditionTrue && condition.ObservedGeneration == route.Generation {
											valid = true
										}
									}
									if !valid {
										return fmt.Errorf("parent %s lacks current %s=True: %v", name, typ, parent.Conditions)
									}
								}
							}
							if !found {
								return fmt.Errorf("missing parent %s for controller %s", name, controller)
							}
						}
						if version != route.ResourceVersion {
							version = route.ResourceVersion
							return fmt.Errorf("waiting for status writes to settle")
						}
						return nil
					}, retry.Timeout(30*time.Second), retry.Delay(100*time.Millisecond), retry.Converge(20))
				}
				check(stableParent, canaryParent)
				// Transfer a parent, then change the Route generation to require a new status from its new owner.
				if parentKind == "Gateway" {
					if err := kuberetry.RetryOnConflict(kuberetry.DefaultRetry, func() error {
						gateway, err := client.Gateways(ns.Name()).Get(context.Background(), "canary", v1.GetOptions{})
						if err != nil {
							return err
						}
						gateway.Labels["istio.io/rev"] = "stable"
						_, err = client.Gateways(ns.Name()).Update(context.Background(), gateway, v1.UpdateOptions{})
						return err
					}); err != nil {
						t.Fatal(err)
					}
				}
				updateRoute := func(change func(*gw.HTTPRoute)) {
					t.Helper()
					if err := kuberetry.RetryOnConflict(kuberetry.DefaultRetry, func() error {
						route, err := routes.Get(context.Background(), "shared", v1.GetOptions{})
						if err != nil {
							return err
						}
						change(route)
						_, err = routes.Update(context.Background(), route, v1.UpdateOptions{})
						return err
					}); err != nil {
						t.Fatal(err)
					}
				}
				updateRoute(func(route *gw.HTTPRoute) {
					hostname := gw.PreciseHostname("updated.example.com")
					route.Spec.Rules[0].Filters[0].RequestRedirect.Hostname = &hostname
					if parentKind == "Service" {
						// Service parents retain Route ownership, even across waypoint revisions.
						route.Labels["istio.io/rev"] = "canary"
					}
				})
				check(stableParent, canaryParent)
				updateRoute(func(route *gw.HTTPRoute) { route.Spec.ParentRefs = route.Spec.ParentRefs[:1] })
				check(stableParent)
			})
		}
	})
}
