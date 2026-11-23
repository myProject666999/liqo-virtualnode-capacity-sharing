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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liqotech/liqo/pkg/consts"
)

// nodeForTest forges a provider node with the given allocatable resources and conditions.
func nodeForTest(name string, allocatable corev1.ResourceList, mutators ...func(*corev1.Node)) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Allocatable: allocatable.DeepCopy(),
			Conditions: []corev1.NodeCondition{{
				Type:   corev1.NodeReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	for _, mutate := range mutators {
		mutate(node)
	}
	return node
}

// podForTest forges a pod scheduled on the given node, with the given requests.
func podForTest(name, nodeName string, requests corev1.ResourceList, mutators ...func(*corev1.Pod)) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{
				Name: "container",
				Resources: corev1.ResourceRequirements{
					Requests: requests.DeepCopy(),
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, mutate := range mutators {
		mutate(pod)
	}
	return pod
}

// liqoPodForTest forges a pod created by Liqo from a ShadowPod.
func liqoPodForTest(name, nodeName string, requests corev1.ResourceList) *corev1.Pod {
	return podForTest(name, nodeName, requests, func(pod *corev1.Pod) {
		pod.Labels = map[string]string{consts.ManagedByLabelKey: consts.ManagedByShadowPodValue}
	})
}

// resourceList is a shorthand to forge a resource list.
func resourceList(items ...any) corev1.ResourceList {
	list := corev1.ResourceList{}
	for i := 0; i < len(items); i += 2 {
		list[items[i].(corev1.ResourceName)] = resource.MustParse(items[i+1].(string))
	}
	return list
}

// expectQuantity asserts that the quantity in the given list is equal to the expected value.
func expectQuantity(list corev1.ResourceList, name corev1.ResourceName, expected string) {
	quantity, ok := list[name]
	ExpectWithOffset(1, ok).To(BeTrue(), "resource %q is missing", name)
	ExpectWithOffset(1, quantity.Cmp(resource.MustParse(expected))).To(Equal(0),
		"resource %q: expected %s, got %s", name, expected, quantity.String())
}

var _ = Describe("ComputeSharedCapacity", func() {
	var (
		result *CapacitySharingResult
		err    error
	)

	standardAllocatable := resourceList(
		corev1.ResourceCPU, "8",
		corev1.ResourceMemory, "16Gi",
		corev1.ResourcePods, "110",
	)

	Describe("ratio validation", func() {
		When("the ratio is zero", func() {
			It("should reject the computation", func() {
				_, err := ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{*nodeForTest("node", standardAllocatable)},
					Ratio: 0,
				})
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("ratio"))
			})
		})

		When("the ratio is out of range", func() {
			It("should reject the computation", func() {
				_, err := ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{*nodeForTest("node", standardAllocatable)},
					Ratio: 101,
				})
				Expect(err).To(HaveOccurred())
			})
		})

		When("the basis is unknown", func() {
			It("should reject the computation", func() {
				_, err := ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{*nodeForTest("node", standardAllocatable)},
					Ratio: 50,
					Basis: RatioBasis("unknown"),
				})
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("node eligibility", func() {
		JustBeforeEach(func() {
			result, err = ComputeSharedCapacity(CapacitySharingInput{
				Nodes: []corev1.Node{
					*nodeForTest("ready", standardAllocatable),
					*nodeForTest("not-ready", standardAllocatable, func(node *corev1.Node) {
						node.Status.Conditions[0].Status = corev1.ConditionFalse
					}),
					*nodeForTest("cordoned", standardAllocatable, func(node *corev1.Node) {
						node.Spec.Unschedulable = true
					}),
					*nodeForTest("tainted", standardAllocatable, func(node *corev1.Node) {
						node.Spec.Taints = []corev1.Taint{{
							Key:    "dedicated",
							Value:  "gpu",
							Effect: corev1.TaintEffectNoSchedule,
						}}
					}),
				},
				Ratio: 100,
			})
			Expect(err).ToNot(HaveOccurred())
		})

		It("should only consider the ready, schedulable and taint-tolerable nodes", func() {
			Expect(result.PerNode).To(HaveLen(1))
			Expect(result.PerNode[0].NodeName).To(Equal("ready"))
			Expect(result.ExcludedNodes).To(ConsistOf(
				NodeExclusion{NodeName: "not-ready", Reason: "node is not ready"},
				NodeExclusion{NodeName: "cordoned", Reason: "node is unschedulable (cordoned)"},
				NodeExclusion{NodeName: "tainted", Reason: `node has an untolerated NoSchedule taint "dedicated"`},
			))
		})

		When("an explicit toleration is provided", func() {
			It("should include the tainted node without excluding the others", func() {
				tolerated, err := ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{
						*nodeForTest("tainted", standardAllocatable, func(node *corev1.Node) {
							node.Spec.Taints = []corev1.Taint{{
								Key:    "dedicated",
								Value:  "gpu",
								Effect: corev1.TaintEffectNoSchedule,
							}}
						}),
					},
					Ratio: 100,
					Tolerations: []corev1.Toleration{{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "gpu",
						Effect:   corev1.TaintEffectNoSchedule,
					}},
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(tolerated.PerNode).To(HaveLen(1))
				Expect(tolerated.ExcludedNodes).To(BeEmpty())
			})
		})
	})

	Describe("the ratio application", func() {
		BeforeEach(func() {
			// One node with 8 CPU, 16Gi memory and 10 pods, running a single non-Liqo pod requesting 2 CPU and 4Gi.
			nodes := []corev1.Node{*nodeForTest("node",
				resourceList(corev1.ResourceCPU, "8", corev1.ResourceMemory, "16Gi", corev1.ResourcePods, "10"))}
			pods := []corev1.Pod{*podForTest("local", "node", resourceList(corev1.ResourceCPU, "2", corev1.ResourceMemory, "4Gi"))}

			// Schedulable: 6 CPU, 12Gi, 9 pods. Granted to other consumers: 2 CPU, 2Gi, 2 pods.
			input := CapacitySharingInput{
				Nodes:   nodes,
				Pods:    pods,
				Granted: resourceList(corev1.ResourceCPU, "2", corev1.ResourceMemory, "2Gi", corev1.ResourcePods, "2"),
				Ratio:   50,
			}
			result, err = ComputeSharedCapacity(input)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should subtract the pods and the grants from the schedulable capacity", func() {
			expectQuantity(result.Schedulable, corev1.ResourceCPU, "6")
			expectQuantity(result.Schedulable, corev1.ResourceMemory, "12Gi")
			expectQuantity(result.Schedulable, corev1.ResourcePods, "9")
			expectQuantity(result.Granted, corev1.ResourceCPU, "2")
		})

		It("should advertise the available basis by default", func() {
			Expect(result.Basis).To(Equal(RatioBasisAvailable))
			// Available: 4 CPU, 10Gi, 7 pods. 50% of those.
			expectQuantity(result.Resources, corev1.ResourceCPU, "2")
			expectQuantity(result.Resources, corev1.ResourceMemory, "5Gi")
			expectQuantity(result.Resources, corev1.ResourcePods, "3")
		})

		It("should report both bases on the same data", func() {
			// Total basis: 50% of 6 CPU = 3 CPU, minus 2 CPU granted = 1 CPU.
			expectQuantity(result.AvailableBasisResources, corev1.ResourceCPU, "2")
			expectQuantity(result.TotalBasisResources, corev1.ResourceCPU, "1")
			// Total basis: 50% of 12Gi = 6Gi, minus 2Gi granted = 4Gi.
			expectQuantity(result.AvailableBasisResources, corev1.ResourceMemory, "5Gi")
			expectQuantity(result.TotalBasisResources, corev1.ResourceMemory, "4Gi")
			// Total basis: 50% of 9 pods = 4 pods, minus 2 granted = 2 pods.
			expectQuantity(result.AvailableBasisResources, corev1.ResourcePods, "3")
			expectQuantity(result.TotalBasisResources, corev1.ResourcePods, "2")
		})

		It("should never advertise more than the available capacity", func() {
			for _, name := range sortedResourceNames(result.Resources, result.Schedulable) {
				advertised := result.Resources[name]
				schedulable := result.Schedulable[name]
				Expect(advertised.Cmp(schedulable) <= 0).To(BeTrue(),
					"resource %q: advertised %s exceeds schedulable %s",
					name, advertised.String(), schedulable.String())
			}
		})

		When("the total basis is selected", func() {
			It("should populate Resources with the total basis", func() {
				total, err := ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{*nodeForTest("node",
						resourceList(corev1.ResourceCPU, "8", corev1.ResourceMemory, "16Gi", corev1.ResourcePods, "10"))},
					Pods:    []corev1.Pod{*podForTest("local", "node", resourceList(corev1.ResourceCPU, "2", corev1.ResourceMemory, "4Gi"))},
					Granted: resourceList(corev1.ResourceCPU, "2", corev1.ResourceMemory, "2Gi", corev1.ResourcePods, "2"),
					Ratio:   50,
					Basis:   RatioBasisTotal,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(total.Basis).To(Equal(RatioBasisTotal))
				expectQuantity(total.Resources, corev1.ResourceCPU, "1")
				expectQuantity(total.TotalBasisResources, corev1.ResourceCPU, "1")
			})
		})
	})

	Describe("the granted resources accounting", func() {
		It("should not deduct the granted resources twice", func() {
			// A Liqo-managed pod consumes the granted resources: the grant must be deducted once, not twice.
			nodes := []corev1.Node{*nodeForTest("node",
				resourceList(corev1.ResourceCPU, "8", corev1.ResourceMemory, "16Gi"))}
			pods := []corev1.Pod{*liqoPodForTest("liqo-pod", "node", resourceList(corev1.ResourceCPU, "2", corev1.ResourceMemory, "4Gi"))}
			granted := resourceList(corev1.ResourceCPU, "2", corev1.ResourceMemory, "4Gi")

			withGrant, err := ComputeSharedCapacity(CapacitySharingInput{
				Nodes: nodes, Pods: pods, Granted: granted, Ratio: 100,
			})
			Expect(err).ToNot(HaveOccurred())
			// Schedulable ignores Liqo pods: 8 CPU, 16Gi. Only the grant is subtracted.
			expectQuantity(withGrant.Resources, corev1.ResourceCPU, "6")
			expectQuantity(withGrant.Resources, corev1.ResourceMemory, "12Gi")

			withoutGrant, err := ComputeSharedCapacity(CapacitySharingInput{Nodes: nodes, Pods: pods, Ratio: 100})
			Expect(err).ToNot(HaveOccurred())
			// Without grants the whole capacity would be shared, as the Liqo pod is not subtracted.
			expectQuantity(withoutGrant.Resources, corev1.ResourceCPU, "8")
		})

		It("should not deduct the resource usage of the other consumers", func() {
			usage := resourceList(corev1.ResourceCPU, "6", corev1.ResourceMemory, "12Gi")
			withUsage, err := ComputeSharedCapacity(CapacitySharingInput{
				Nodes: []corev1.Node{*nodeForTest("node", resourceList(corev1.ResourceCPU, "8", corev1.ResourceMemory, "16Gi"))},
				Usage: usage,
				Ratio: 100,
			})
			Expect(err).ToNot(HaveOccurred())
			// Usage is only used to detect overcommit, not to compute the capacity.
			expectQuantity(withUsage.Resources, corev1.ResourceCPU, "8")
		})
	})

	Describe("the rounding", func() {
		It("should truncate the CPU to millicores", func() {
			// 999m with a 50% ratio is 499.5m, truncated to 499m.
			result, err := ComputeSharedCapacity(CapacitySharingInput{
				Nodes: []corev1.Node{*nodeForTest("node", resourceList(corev1.ResourceCPU, "999m", corev1.ResourceMemory, "1Gi"))},
				Ratio: 50,
			})
			Expect(err).ToNot(HaveOccurred())
			expectQuantity(result.Resources, corev1.ResourceCPU, "499m")
		})

		It("should truncate the memory to MiB", func() {
			// 1000Mi with a 50% ratio is 500Mi; 1001m of memory is truncated to whichever MiB boundary is below.
			result, err := ComputeSharedCapacity(CapacitySharingInput{
				Nodes: []corev1.Node{*nodeForTest("node", resourceList(corev1.ResourceMemory, "1000Mi", corev1.ResourceCPU, "1"))},
				Ratio: 33,
			})
			Expect(err).ToNot(HaveOccurred())
			// 1000Mi * 33% = 330Mi.
			expectQuantity(result.Resources, corev1.ResourceMemory, "330Mi")
		})

		It("should truncate the extended resources to the integer unit", func() {
			result, err := ComputeSharedCapacity(CapacitySharingInput{
				Nodes: []corev1.Node{*nodeForTest("node",
					resourceList(corev1.ResourceCPU, "1", corev1.ResourceMemory, "1Gi", corev1.ResourceName("nvidia.com/gpu"), "5"))},
				Ratio: 50,
			})
			Expect(err).ToNot(HaveOccurred())
			// 5 GPUs * 50% = 2.5, truncated to 2.
			expectQuantity(result.Resources, corev1.ResourceName("nvidia.com/gpu"), "2")
		})
	})

	Describe("the extended resource fragmentation", func() {
		BeforeEach(func() {
			result, err = ComputeSharedCapacity(CapacitySharingInput{
				Nodes: []corev1.Node{
					*nodeForTest("node-a", resourceList(
						corev1.ResourceCPU, "4", corev1.ResourceMemory, "8Gi", corev1.ResourceName("nvidia.com/gpu"), "2")),
					*nodeForTest("node-b", resourceList(
						corev1.ResourceCPU, "4", corev1.ResourceMemory, "8Gi", corev1.ResourceName("nvidia.com/gpu"), "2")),
				},
				Ratio: 100,
			})
			Expect(err).ToNot(HaveOccurred())
		})

		It("should report the aggregate and the single-node maximum", func() {
			expectQuantity(result.Resources, corev1.ResourceName("nvidia.com/gpu"), "4")
			expectQuantity(result.SingleNodeMax, corev1.ResourceName("nvidia.com/gpu"), "2")
		})

		It("should explain the fragmentation limitation", func() {
			Expect(result.FragmentationReason).To(ContainSubstring("nvidia.com/gpu"))
			Expect(result.FragmentationReason).To(ContainSubstring("single node"))
		})

		When("all the GPUs fit on a single node", func() {
			It("should not report any fragmentation", func() {
				notFragmented, err := ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{*nodeForTest("node", resourceList(
						corev1.ResourceCPU, "4", corev1.ResourceMemory, "8Gi", corev1.ResourceName("nvidia.com/gpu"), "4"))},
					Ratio: 100,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(notFragmented.FragmentationReason).To(BeEmpty())
			})
		})
	})

	Describe("the readiness", func() {
		When("the provider has no CPU to share", func() {
			BeforeEach(func() {
				result, err = ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{*nodeForTest("node",
						resourceList(corev1.ResourceMemory, "8Gi", corev1.ResourcePods, "10"))},
					Ratio: 100,
				})
				Expect(err).ToNot(HaveOccurred())
			})

			It("should report the capacity as not ready, explaining why", func() {
				Expect(result.Ready).To(BeFalse())
				Expect(result.NotReadyReason).To(ContainSubstring("CPU"))
			})
		})

		When("the provider has no eligible node", func() {
			It("should report the exclusion reason", func() {
				notReady, err := ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{*nodeForTest("cordoned", standardAllocatable, func(node *corev1.Node) {
						node.Spec.Unschedulable = true
					})},
					Ratio: 100,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(notReady.Ready).To(BeFalse())
				Expect(notReady.NotReadyReason).To(ContainSubstring("no eligible node"))
				Expect(notReady.NotReadyReason).To(ContainSubstring("cordoned"))
			})
		})

		When("the CPU and memory are exhausted by the running pods", func() {
			It("should report the underlying capacity as the reason", func() {
				notReady, err := ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{*nodeForTest("node", standardAllocatable)},
					Pods: []corev1.Pod{*podForTest("heavy", "node", resourceList(
						corev1.ResourceCPU, "8", corev1.ResourceMemory, "16Gi"))},
					Ratio: 100,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(notReady.Ready).To(BeFalse())
				Expect(notReady.NotReadyReason).To(ContainSubstring("no schedulable capacity"))
			})
		})
	})

	Describe("the overcommit detection", func() {
		When("the usage is above the advertised capacity", func() {
			BeforeEach(func() {
				result, err = ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{*nodeForTest("node", standardAllocatable)},
					Usage: resourceList(corev1.ResourceCPU, "5"),
					Ratio: 50,
				})
				Expect(err).ToNot(HaveOccurred())
			})

			It("should report the overcommit, without evicting anything", func() {
				Expect(result.Overcommitted).To(BeTrue())
				Expect(result.OvercommitReason).To(ContainSubstring("not evicted"))
				Expect(result.OvercommitReason).To(ContainSubstring("cpu"))
			})
		})

		When("the usage is below the advertised capacity", func() {
			BeforeEach(func() {
				result, err = ComputeSharedCapacity(CapacitySharingInput{
					Nodes: []corev1.Node{*nodeForTest("node", standardAllocatable)},
					Usage: resourceList(corev1.ResourceCPU, "1", corev1.ResourceMemory, "2Gi"),
					Ratio: 100,
				})
				Expect(err).ToNot(HaveOccurred())
			})

			It("should not report any overcommit", func() {
				Expect(result.Overcommitted).To(BeFalse())
				Expect(result.OvercommitReason).To(BeEmpty())
			})
		})
	})

	Describe("the per-node output", func() {
		BeforeEach(func() {
			result, err = ComputeSharedCapacity(CapacitySharingInput{
				Nodes: []corev1.Node{
					*nodeForTest("node-b", resourceList(corev1.ResourceCPU, "4", corev1.ResourceMemory, "8Gi")),
					*nodeForTest("node-a", resourceList(corev1.ResourceCPU, "4", corev1.ResourceMemory, "8Gi")),
				},
				Granted: resourceList(corev1.ResourceCPU, "4"),
				Ratio:   50,
			})
			Expect(err).ToNot(HaveOccurred())
		})

		It("should sort the nodes by name", func() {
			Expect(result.PerNode).To(HaveLen(2))
			Expect(result.PerNode[0].NodeName).To(Equal("node-a"))
			Expect(result.PerNode[1].NodeName).To(Equal("node-b"))
		})

		It("should allocate the whole grant only once", func() {
			total := *resource.NewQuantity(0, resource.DecimalSI)
			for i := range result.PerNode {
				total.Add(result.PerNode[i].GrantedShare[corev1.ResourceCPU])
			}
			Expect(total.Cmp(resource.MustParse("4"))).To(Equal(0))
		})

		It("should make the per-node sum differ from the aggregate only by allocation and rounding", func() {
			cpuSum := *resource.NewQuantity(0, resource.DecimalSI)
			for i := range result.PerNode {
				cpuSum.Add(result.PerNode[i].Shared[corev1.ResourceCPU])
			}
			// Aggregate: (8-4) * 50% = 2 CPU. Per node: node-a available 0 => 0; node-b available 4 => 2.
			Expect(cpuSum.Cmp(resource.MustParse("2"))).To(Equal(0))
		})
	})
})
