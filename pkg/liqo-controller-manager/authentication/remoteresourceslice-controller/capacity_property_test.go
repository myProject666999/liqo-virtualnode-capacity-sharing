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
	"math/rand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var _ = Describe("Capacity sharing properties", func() {
	// fixedSeed makes the generated scenarios reproducible.
	const fixedSeed = 15510

	allocatableWithPods := func(cpu, memory string, pods string) corev1.ResourceList {
		return resourceList(corev1.ResourceCPU, cpu, corev1.ResourceMemory, memory, corev1.ResourcePods, pods)
	}

	DescribeTable("the ratio matrix",
		func(ratio int, expectedCPU, expectedMemory string) {
			result, err := ComputeSharedCapacity(CapacitySharingInput{
				Nodes: []corev1.Node{*nodeForTest("node", allocatableWithPods("8", "16Gi", "100"))},
				Ratio: ratio,
			})
			Expect(err).ToNot(HaveOccurred())
			expectQuantity(result.Resources, corev1.ResourceCPU, expectedCPU)
			expectQuantity(result.Resources, corev1.ResourceMemory, expectedMemory)
		},
		Entry("1 percent", 1, "80m", "163Mi"),
		Entry("50 percent", 50, "4", "8Gi"),
		Entry("100 percent", 100, "8", "16Gi"),
	)

	Describe("the oversell handling", func() {
		It("should advertise a capacity below the current usage, without evicting", func() {
			// The provider runs 20 CPU worth of local pods on an 8-CPU node: no capacity is left to share.
			result, err := ComputeSharedCapacity(CapacitySharingInput{
				Nodes: []corev1.Node{*nodeForTest("node", allocatableWithPods("8", "16Gi", "100"))},
				Pods: []corev1.Pod{*podForTest("heavy", "node", resourceList(
					corev1.ResourceCPU, "20", corev1.ResourceMemory, "40Gi"))},
				Usage: resourceList(corev1.ResourceCPU, "6", corev1.ResourceMemory, "12Gi"),
				Ratio: 100,
			})
			Expect(err).ToNot(HaveOccurred())
			// The remaining capacity is clamped to zero before summing.
			expectQuantity(result.Schedulable, corev1.ResourceCPU, "0")
			expectQuantity(result.Resources, corev1.ResourceCPU, "0")
			Expect(result.Overcommitted).To(BeTrue())
			Expect(result.OvercommitReason).To(ContainSubstring("not evicted"))
			// A null capacity is not ready, and this is reported through the shared readiness criterion.
			Expect(result.Ready).To(BeFalse())
			Expect(result.NotReadyReason).To(ContainSubstring("no schedulable capacity"))
		})
	})

	Describe("the publication thresholds", func() {
		previous := resourceList(corev1.ResourceCPU, "4")

		It("should always publish with the default thresholds", func() {
			publish, changed := DefaultPublishThresholds().ShouldPublish(previous,
				resourceList(corev1.ResourceCPU, "4010m"))
			Expect(publish).To(BeTrue())
			Expect(changed).To(ConsistOf("cpu"))
		})

		It("should never publish an identical capacity", func() {
			publish, _ := DefaultPublishThresholds().ShouldPublish(previous, previous.DeepCopy())
			Expect(publish).To(BeFalse())
		})

		It("should suppress a change below the absolute threshold", func() {
			thresholds := PublishThresholds{Absolute: resourceList(corev1.ResourceCPU, "100m")}
			publish, _ := thresholds.ShouldPublish(previous, resourceList(corev1.ResourceCPU, "4050m"))
			Expect(publish).To(BeFalse())

			publish, _ = thresholds.ShouldPublish(previous, resourceList(corev1.ResourceCPU, "4200m"))
			Expect(publish).To(BeTrue())
		})

		It("should suppress a change below the relative threshold", func() {
			thresholds := PublishThresholds{Relative: 0.10}
			publish, _ := thresholds.ShouldPublish(previous, resourceList(corev1.ResourceCPU, "4300m"))
			Expect(publish).To(BeFalse())

			publish, _ = thresholds.ShouldPublish(previous, resourceList(corev1.ResourceCPU, "4500m"))
			Expect(publish).To(BeTrue())
		})

		It("should publish any change from a zero baseline", func() {
			// A resource appearing from zero has no relative baseline, so the change is always published.
			thresholds := PublishThresholds{Relative: 0.10}
			publish, _ := thresholds.ShouldPublish(corev1.ResourceList{}, resourceList(corev1.ResourceCPU, "1m"))
			Expect(publish).To(BeTrue())
		})

		It("should accumulate sub-threshold changes instead of never publishing", func() {
			// Each step is below the absolute threshold, but comparing against the last published value
			// eventually crosses it.
			thresholds := PublishThresholds{Absolute: resourceList(corev1.ResourceCPU, "500m")}
			lastPublished := resourceList(corev1.ResourceCPU, "4")

			publishedSteps := 0
			for step := 1; step <= 10; step++ {
				candidate := resourceList(corev1.ResourceCPU,
					resource.NewMilliQuantity(int64(4000+step*100), resource.DecimalSI).String())
				if publish, _ := thresholds.ShouldPublish(lastPublished, candidate); publish {
					lastPublished = candidate
					publishedSteps++
				}
			}
			Expect(publishedSteps).To(BeNumerically(">", 0))
		})
	})

	Describe("property-based checks", func() {
		// randomScenario generates a reproducible capacity sharing scenario.
		newScenario := func(random *rand.Rand) CapacitySharingInput {
			nodeCount := 1 + random.Intn(4)
			nodes := make([]corev1.Node, nodeCount)
			pods := make([]corev1.Pod, 0)
			for i := range nodes {
				name := "node-" + string(rune('a'+i))
				cpu := 1000 * (1 + random.Intn(16))
				memoryMi := 512 * (1 + random.Intn(32))
				nodes[i] = *nodeForTest(name, resourceList(
					corev1.ResourceCPU, resource.NewMilliQuantity(int64(cpu), resource.DecimalSI).String(),
					corev1.ResourceMemory, resource.NewQuantity(int64(memoryMi)*1024*1024, resource.BinarySI).String(),
					corev1.ResourcePods, resource.NewQuantity(int64(10+random.Intn(100)), resource.DecimalSI).String(),
				))

				for p := 0; p < random.Intn(3); p++ {
					pods = append(pods, *podForTest("pod", nodes[i].GetName(), resourceList(
						corev1.ResourceCPU, resource.NewMilliQuantity(int64(100*random.Intn(4)), resource.DecimalSI).String(),
						corev1.ResourceMemory, resource.NewQuantity(int64(128*random.Intn(4))*1024*1024, resource.BinarySI).String(),
					)))
				}
			}

			granted := resourceList(
				corev1.ResourceCPU, resource.NewMilliQuantity(int64(100*random.Intn(10)), resource.DecimalSI).String(),
			)
			return CapacitySharingInput{
				Nodes:   nodes,
				Pods:    pods,
				Granted: granted,
				Ratio:   1 + random.Intn(100),
			}
		}

		It("should be monotonic in the ratio", func() {
			random := rand.New(rand.NewSource(fixedSeed))
			for i := 0; i < 50; i++ {
				scenario := newScenario(random)

				previous := corev1.ResourceList{}
				for ratio := 1; ratio <= 100; ratio++ {
					scenario.Ratio = ratio
					result, err := ComputeSharedCapacity(scenario)
					Expect(err).ToNot(HaveOccurred())

					for _, name := range sortedResourceNames(result.Resources) {
						quantity := result.Resources[name]
						if before, ok := previous[name]; ok {
							Expect(quantity.Cmp(before) >= 0).To(BeTrue(),
								"ratio %d: resource %q decreased from %s to %s",
								ratio, name, before.String(), quantity.String())
						}
					}
					previous = result.Resources
				}
			}
		})

		It("should never advertise more than the schedulable capacity", func() {
			random := rand.New(rand.NewSource(fixedSeed))
			for i := 0; i < 200; i++ {
				result, err := ComputeSharedCapacity(newScenario(random))
				Expect(err).ToNot(HaveOccurred())

				for _, name := range sortedResourceNames(result.Resources, result.Schedulable) {
					advertised := result.Resources[name]
					schedulable := result.Schedulable[name]
					Expect(advertised.Cmp(schedulable) <= 0).To(BeTrue(),
						"scenario %d: resource %q advertised %s exceeds schedulable %s",
						i, name, advertised.String(), schedulable.String())
				}
			}
		})

		It("should advertise at most the available capacity on both bases", func() {
			random := rand.New(rand.NewSource(fixedSeed))
			for i := 0; i < 200; i++ {
				result, err := ComputeSharedCapacity(newScenario(random))
				Expect(err).ToNot(HaveOccurred())

				for _, name := range sortedResourceNames(result.Schedulable, result.Granted) {
					schedulable := result.Schedulable[name]
					granted := result.Granted[name]
					available := schedulable.DeepCopy()
					available.Sub(granted)

					for basis, advertised := range map[RatioBasis]corev1.ResourceList{
						RatioBasisAvailable: result.AvailableBasisResources,
						RatioBasisTotal:     result.TotalBasisResources,
					} {
						quantity := advertised[name]
						Expect(quantity.Sign() >= 0).To(BeTrue(),
							"scenario %d (%s): resource %q advertised %s is negative",
							i, basis, name, quantity.String())
						Expect(quantity.Cmp(available) <= 0).To(BeTrue(),
							"scenario %d (%s): resource %q advertised %s exceeds available %s",
							i, basis, name, quantity.String(), available.String())
					}
				}
			}
		})

		It("should never deduct the granted amount more than once per node", func() {
			random := rand.New(rand.NewSource(fixedSeed))
			for i := 0; i < 200; i++ {
				result, err := ComputeSharedCapacity(newScenario(random))
				Expect(err).ToNot(HaveOccurred())

				for _, name := range sortedResourceNames(result.Granted) {
					total := *resource.NewQuantity(0, resource.DecimalSI)
					for j := range result.PerNode {
						if share, ok := result.PerNode[j].GrantedShare[name]; ok {
							total.Add(share)
						}
					}
					granted := result.Granted[name]
					Expect(total.Cmp(granted) <= 0).To(BeTrue(),
						"scenario %d: resource %q granted share total %s exceeds the granted amount %s",
						i, name, total.String(), granted.String())
				}
			}
		})

		It("should be deterministic and not mutate the input", func() {
			random := rand.New(rand.NewSource(fixedSeed))
			for i := 0; i < 100; i++ {
				scenario := newScenario(random)
				nodesBefore := scenario.Nodes
				podsBefore := scenario.Pods
				grantedBefore := scenario.Granted.DeepCopy()

				first, err := ComputeSharedCapacity(scenario)
				Expect(err).ToNot(HaveOccurred())
				second, err := ComputeSharedCapacity(scenario)
				Expect(err).ToNot(HaveOccurred())

				Expect(second.Resources).To(Equal(first.Resources))
				Expect(second.PerNode).To(Equal(first.PerNode))

				Expect(scenario.Nodes).To(Equal(nodesBefore))
				Expect(scenario.Pods).To(Equal(podsBefore))
				Expect(scenario.Granted).To(Equal(grantedBefore))
			}
		})
	})
})
