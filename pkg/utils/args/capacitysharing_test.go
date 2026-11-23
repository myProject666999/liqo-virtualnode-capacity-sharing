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

package args

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("CapacitySharingPolicyList", func() {
	It("should parse a single policy with the default basis", func() {
		var list CapacitySharingPolicyList
		Expect(list.Set("shared=50")).To(Succeed())
		Expect(list.Policies).To(HaveLen(1))
		Expect(list.Policies[0]).To(Equal(CapacitySharingPolicyName{Class: "shared", Ratio: 50}))
	})

	It("should parse multiple policies and an explicit basis", func() {
		var list CapacitySharingPolicyList
		Expect(list.Set("shared=50,dedicated=100:Total")).To(Succeed())
		Expect(list.Policies).To(ConsistOf(
			CapacitySharingPolicyName{Class: "shared", Ratio: 50},
			CapacitySharingPolicyName{Class: "dedicated", Ratio: 100, Basis: "Total"},
		))
	})

	It("should leave an empty value untouched", func() {
		var list CapacitySharingPolicyList
		Expect(list.Set("")).To(Succeed())
		Expect(list.Policies).To(BeEmpty())
	})

	DescribeTable("invalid policies",
		func(value string) {
			var list CapacitySharingPolicyList
			Expect(list.Set(value)).ToNot(Succeed())
		},
		Entry("zero ratio", "shared=0"),
		Entry("ratio above range", "shared=101"),
		Entry("missing ratio", "shared="),
		Entry("missing class", "=50"),
		Entry("missing separator", "shared"),
		Entry("negative ratio", "shared=-1"),
	)
})

var _ = Describe("TolerationList", func() {
	It("should parse a key/value/effect toleration", func() {
		var list TolerationList
		Expect(list.Set("dedicated=gpu:NoSchedule")).To(Succeed())
		Expect(list.Tolerations).To(ConsistOf(corev1.Toleration{
			Key:      "dedicated",
			Operator: corev1.TolerationOpEqual,
			Value:    "gpu",
			Effect:   corev1.TaintEffectNoSchedule,
		}))
	})

	It("should parse an exists toleration", func() {
		var list TolerationList
		Expect(list.Set("dedicated:NoSchedule")).To(Succeed())
		Expect(list.Tolerations).To(ConsistOf(corev1.Toleration{
			Key:      "dedicated",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		}))
	})

	It("should parse multiple tolerations", func() {
		var list TolerationList
		Expect(list.Set("a=b:NoSchedule,c:NoExecute")).To(Succeed())
		Expect(list.Tolerations).To(HaveLen(2))
	})

	DescribeTable("invalid tolerations",
		func(value string) {
			var list TolerationList
			Expect(list.Set(value)).ToNot(Succeed())
		},
		Entry("invalid effect", "dedicated=gpu:Invalid"),
	)
})
