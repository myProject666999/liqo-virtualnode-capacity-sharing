// Copyright 2019-2026 The Liqo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remoteresourceslicecontroller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	authv1beta1 "github.com/liqotech/liqo/apis/authentication/v1beta1"
	liqov1beta1 "github.com/liqotech/liqo/apis/core/v1beta1"
	"github.com/liqotech/liqo/pkg/liqo-controller-manager/authentication"
	"github.com/liqotech/liqo/pkg/virtualKubelet/forge"
)

// capacitySharingScheme returns the scheme needed by the fake client.
func capacitySharingScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	Expect(corev1.AddToScheme(scheme)).To(Succeed())
	Expect(authv1beta1.AddToScheme(scheme)).To(Succeed())
	return scheme
}

// providerSlice forges an accepted ResourceSlice belonging to another consumer, advertising the given resources.
func providerSlice(name string, resources corev1.ResourceList) *authv1beta1.ResourceSlice {
	slice := &authv1beta1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "liqo-tenant-other"},
		Status: authv1beta1.ResourceSliceStatus{
			Resources: resources,
		},
	}
	authentication.EnsureCondition(slice, authv1beta1.ResourceSliceConditionTypeResources,
		authv1beta1.ResourceSliceConditionAccepted, "ResourceSliceResourcesAccepted", "accepted")
	return slice
}

var _ = Describe("HandleResourcesStatus capacity sharing", func() {
	const sharedClass = authv1beta1.ResourceSliceClass("shared")

	var (
		consumerClusterID liqov1beta1.ClusterID
		ctx               context.Context
		reconciler        *RemoteResourceSliceReconciler
		slice             *authv1beta1.ResourceSlice
		tenant            *authv1beta1.Tenant
		objects           []client.Object
	)

	newReconciler := func(objects ...client.Object) *RemoteResourceSliceReconciler {
		fakeClient := fake.NewClientBuilder().
			WithScheme(capacitySharingScheme()).
			WithObjects(objects...).
			Build()
		return &RemoteResourceSliceReconciler{
			Client:        fakeClient,
			eventRecorder: record.NewFakeRecorder(100),
			sliceStatusOptions: &SliceStatusOptions{
				DefaultResourceSliceClassEnabled: true,
				DefaultResourceQuantity:          resourceList(corev1.ResourceCPU, "4", corev1.ResourceMemory, "8Gi"),
				CapacitySharingPolicies: map[authv1beta1.ResourceSliceClass]CapacitySharingPolicy{
					sharedClass: {Ratio: 50},
				},
			},
			reconciledClasses: []authv1beta1.ResourceSliceClass{
				authv1beta1.ResourceSliceClassDefault,
				authv1beta1.ResourceSliceClassUnknown,
			},
		}
	}

	BeforeEach(func() {
		consumerClusterID = liqov1beta1.ClusterID("consumer")
		ctx = context.Background()
		slice = &authv1beta1.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{Name: "slice", Namespace: "liqo-tenant-consumer"},
			Spec: authv1beta1.ResourceSliceSpec{
				ConsumerClusterID: &consumerClusterID,
				Class:             sharedClass,
				Resources:         resourceList(corev1.ResourceCPU, "2"),
			},
		}
		tenant = &authv1beta1.Tenant{
			ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "liqo-tenant-consumer"},
			Spec:       authv1beta1.TenantSpec{TenantCondition: authv1beta1.TenantConditionActive},
		}
		objects = []client.Object{slice, tenant}
	})

	When("the class enables a capacity sharing policy", func() {
		BeforeEach(func() {
			objects = append(objects,
				nodeForTest("node", resourceList(
					corev1.ResourceCPU, "8", corev1.ResourceMemory, "16Gi", corev1.ResourcePods, "100")),
				// Another consumer already holds 2 CPU through an accepted ResourceSlice.
				providerSlice("other", resourceList(corev1.ResourceCPU, "2", corev1.ResourceMemory, "2Gi")),
			)
			reconciler = newReconciler(objects...)
		})

		It("should grant the computed share and keep the legacy status fields", func() {
			Expect(reconciler.handleResourcesStatus(ctx, slice, tenant)).To(Succeed())

			// Schedulable 8 CPU, 16Gi; granted 2 CPU, 2Gi; available 6 CPU, 14Gi; 50% => 3 CPU, 7Gi.
			expectQuantity(slice.Status.Resources, corev1.ResourceCPU, "3")
			expectQuantity(slice.Status.Resources, corev1.ResourceMemory, "7Gi")

			cond := authentication.GetCondition(slice, authv1beta1.ResourceSliceConditionTypeResources)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(authv1beta1.ResourceSliceConditionAccepted))
		})

		It("should ignore the requested amount, as the capacity is computed from the provider", func() {
			slice.Spec.Resources = resourceList(corev1.ResourceCPU, "100")
			Expect(reconciler.handleResourcesStatus(ctx, slice, tenant)).To(Succeed())
			expectQuantity(slice.Status.Resources, corev1.ResourceCPU, "3")
		})

		It("should keep the previously published capacity when the change is below the threshold", func() {
			reconciler.sliceStatusOptions.CapacitySharingPolicies[sharedClass] = CapacitySharingPolicy{
				Ratio:   50,
				Publish: PublishThresholds{Relative: 0.5},
			}
			slice.Status.Resources = resourceList(corev1.ResourceCPU, "3", corev1.ResourceMemory, "7Gi")

			Expect(reconciler.handleResourcesStatus(ctx, slice, tenant)).To(Succeed())
			// The candidate is identical to the previous capacity: nothing changes.
			expectQuantity(slice.Status.Resources, corev1.ResourceCPU, "3")
		})

		When("the running pods exhaust the provider capacity", func() {
			BeforeEach(func() {
				heavy := podForTest("heavy", "node", resourceList(corev1.ResourceCPU, "8", corev1.ResourceMemory, "16Gi"))
				reconciler = newReconciler(append(objects, heavy)...)
			})

			It("should advertise no usable capacity, reporting the scarcity reason", func() {
				Expect(reconciler.handleResourcesStatus(ctx, slice, tenant)).To(Succeed())
				expectQuantity(slice.Status.Resources, corev1.ResourceCPU, "0")

				cond := authentication.GetCondition(slice, authv1beta1.ResourceSliceConditionTypeResources)
				Expect(cond).ToNot(BeNil())
				Expect(cond.Status).To(Equal(authv1beta1.ResourceSliceConditionAccepted))
				Expect(cond.Reason).To(Equal(resourcesScarceReason))
				Expect(cond.Message).To(ContainSubstring("no schedulable capacity"))
			})
		})

		When("the advertised capacity is below the current usage", func() {
			BeforeEach(func() {
				// A Liqo-managed pod of this consumer consuming more than the 50% share of the 8 CPU node.
				consumerPod := liqoPodForTest("consumer-pod", "node", resourceList(corev1.ResourceCPU, "6"))
				consumerPod.Labels[forge.LiqoOriginClusterIDKey] = string(consumerClusterID)
				reconciler = newReconciler(append(objects, consumerPod)...)
			})

			It("should report the overcommit without evicting the pod", func() {
				Expect(reconciler.handleResourcesStatus(ctx, slice, tenant)).To(Succeed())

				cond := authentication.GetCondition(slice, authv1beta1.ResourceSliceConditionTypeResources)
				Expect(cond).ToNot(BeNil())
				Expect(cond.Reason).To(Equal(resourcesOvercommittedReason))
				Expect(cond.Message).To(ContainSubstring("not evicted"))
				// The Liqo-managed pod is not subtracted from the schedulable capacity.
				expectQuantity(slice.Status.Resources, corev1.ResourceCPU, "3")
			})
		})
	})

	When("the tenant is cordoned", func() {
		BeforeEach(func() {
			tenant.Spec.TenantCondition = authv1beta1.TenantConditionCordoned
			reconciler = newReconciler(objects...)
		})

		It("should deny the resources instead of computing a capacity", func() {
			Expect(reconciler.handleResourcesStatus(ctx, slice, tenant)).To(Succeed())
			cond := authentication.GetCondition(slice, authv1beta1.ResourceSliceConditionTypeResources)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(authv1beta1.ResourceSliceConditionDenied))
			Expect(slice.Status.Resources).To(BeEmpty())
		})
	})

	When("the class does not enable a capacity sharing policy", func() {
		BeforeEach(func() {
			slice.Spec.Class = authv1beta1.ResourceSliceClassDefault
			slice.Spec.Resources = resourceList(corev1.ResourceCPU, "2")
			reconciler = newReconciler(objects...)
		})

		It("should keep the legacy accept-as-requested behavior with the defaults", func() {
			Expect(reconciler.handleResourcesStatus(ctx, slice, tenant)).To(Succeed())
			expectQuantity(slice.Status.Resources, corev1.ResourceCPU, "2")
			expectQuantity(slice.Status.Resources, corev1.ResourceMemory, "8Gi")
		})
	})

	When("the default class is administratively disabled and carries a policy", func() {
		BeforeEach(func() {
			reconciler = newReconciler(objects...)
			reconciler.sliceStatusOptions.DefaultResourceSliceClassEnabled = false
			reconciler.sliceStatusOptions.CapacitySharingPolicies[authv1beta1.ResourceSliceClassDefault] =
				CapacitySharingPolicy{Ratio: 50}
			slice.Spec.Class = authv1beta1.ResourceSliceClassUnknown
		})

		It("should still deny the resources, as the policy cannot bypass the administrative denial", func() {
			Expect(reconciler.handleResourcesStatus(ctx, slice, tenant)).To(Succeed())
			cond := authentication.GetCondition(slice, authv1beta1.ResourceSliceConditionTypeResources)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(authv1beta1.ResourceSliceConditionDenied))
			Expect(cond.Reason).To(Equal("DefaultResourceSliceClassDisabled"))
		})
	})

	When("the default class is enabled and carries a policy", func() {
		BeforeEach(func() {
			objects = append(objects,
				nodeForTest("node", resourceList(
					corev1.ResourceCPU, "8", corev1.ResourceMemory, "16Gi", corev1.ResourcePods, "100")))
			reconciler = newReconciler(objects...)
			reconciler.sliceStatusOptions.CapacitySharingPolicies[authv1beta1.ResourceSliceClassDefault] =
				CapacitySharingPolicy{Ratio: 100}
			slice.Spec.Class = authv1beta1.ResourceSliceClassUnknown
		})

		It("should use the policy instead of accepting the requested amount", func() {
			Expect(reconciler.handleResourcesStatus(ctx, slice, tenant)).To(Succeed())
			expectQuantity(slice.Status.Resources, corev1.ResourceCPU, "8")
			expectQuantity(slice.Status.Resources, corev1.ResourceMemory, "16Gi")
		})
	})
})

var _ = Describe("Usage resources", func() {
	consumerClusterID := liqov1beta1.ClusterID("consumer")

	It("should sum the requests of the Liqo pods of the given consumer only", func() {
		other := liqov1beta1.ClusterID("other")
		pods := []corev1.Pod{
			*liqoPodForTest("mine", "node", resourceList(corev1.ResourceCPU, "1")),
			*liqoPodForTest("theirs", "node", resourceList(corev1.ResourceCPU, "4")),
			*podForTest("local", "node", resourceList(corev1.ResourceCPU, "8")),
		}
		pods[0].Labels[forge.LiqoOriginClusterIDKey] = string(consumerClusterID)
		pods[1].Labels[forge.LiqoOriginClusterIDKey] = string(other)

		usage := usageResources(pods, &consumerClusterID)
		expectQuantity(usage, corev1.ResourceCPU, "1")
	})

	It("should ignore the consumer label on pods not managed by Liqo", func() {
		pod := podForTest("local", "node", resourceList(corev1.ResourceCPU, "8"))
		pod.Labels = map[string]string{forge.LiqoOriginClusterIDKey: string(consumerClusterID)}

		usage := usageResources([]corev1.Pod{*pod}, &consumerClusterID)
		Expect(usage).To(BeEmpty())
	})
})
