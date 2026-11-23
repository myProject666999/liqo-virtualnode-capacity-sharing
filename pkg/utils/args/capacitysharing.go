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
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// CapacitySharingPolicyName is the capacity sharing policy configured for a ResourceSlice class.
// It is kept independent of the controller types, so that it can be used as a flag value.
type CapacitySharingPolicyName struct {
	// Class is the ResourceSlice class the policy applies to.
	Class string
	// Ratio is the integer percentage (1-100) of the provider capacity shared with the consumer.
	Ratio int
	// Basis selects whether the ratio applies to the available or to the total capacity.
	// Empty defaults to the available capacity.
	Basis string
}

// String returns the stringified capacity sharing policy.
func (p CapacitySharingPolicyName) String() string {
	if p.Basis == "" {
		return fmt.Sprintf("%s=%d", p.Class, p.Ratio)
	}
	return fmt.Sprintf("%s=%d:%s", p.Class, p.Ratio, p.Basis)
}

// CapacitySharingPolicyList implements the flag.Value interface and allows to parse stringified capacity
// sharing policies in the form: "class1=50,class2=100:Total".
type CapacitySharingPolicyList struct {
	Policies []CapacitySharingPolicyName
}

// String returns the stringified list.
func (pl CapacitySharingPolicyList) String() string {
	if pl.Policies == nil {
		return ""
	}
	items := make([]string, len(pl.Policies))
	for i := range pl.Policies {
		items[i] = pl.Policies[i].String()
	}
	return strings.Join(items, ",")
}

// Set parses the provided string into the list of capacity sharing policies.
func (pl *CapacitySharingPolicyList) Set(str string) error {
	if str == "" {
		return nil
	}
	for _, chunk := range strings.Split(str, ",") {
		class, ratioAndBasis, ok := strings.Cut(chunk, "=")
		if !ok || class == "" {
			return fmt.Errorf("invalid capacity sharing policy %q: expected <class>=<ratio>[:<basis>]", chunk)
		}

		ratioString, basis, _ := strings.Cut(ratioAndBasis, ":")
		ratio, err := strconv.Atoi(ratioString)
		if err != nil {
			return fmt.Errorf("invalid capacity sharing ratio in %q: %w", chunk, err)
		}
		if ratio < 1 || ratio > 100 {
			return fmt.Errorf("invalid capacity sharing ratio in %q: it must be in the range [1, 100]", chunk)
		}

		pl.Policies = append(pl.Policies, CapacitySharingPolicyName{Class: class, Ratio: ratio, Basis: basis})
	}
	return nil
}

// Type returns the capacitySharingPolicyList type.
func (pl CapacitySharingPolicyList) Type() string {
	return "capacitySharingPolicyList"
}

// TolerationList implements the flag.Value interface and allows to parse stringified tolerations in the form
// "key=value:Effect,key:Effect,key" (repeatable).
type TolerationList struct {
	Tolerations []corev1.Toleration
}

// String returns the stringified list.
func (tl TolerationList) String() string {
	if tl.Tolerations == nil {
		return ""
	}
	items := make([]string, len(tl.Tolerations))
	for i := range tl.Tolerations {
		items[i] = tl.Tolerations[i].String()
	}
	return strings.Join(items, ",")
}

// parseToleration parses a single toleration in the form "key=value:Effect", "key:Effect" or "key".
func parseToleration(str string) (corev1.Toleration, error) {
	toleration := corev1.Toleration{}

	if keyAndEffect, effect, ok := strings.Cut(str, ":"); ok {
		parsedEffect := corev1.TaintEffect(effect)
		switch parsedEffect {
		case corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
			toleration.Effect = parsedEffect
		default:
			return corev1.Toleration{}, fmt.Errorf("invalid toleration effect %q in %q", effect, str)
		}
		str = keyAndEffect
	}

	if key, value, ok := strings.Cut(str, "="); ok {
		toleration.Key = key
		toleration.Value = value
		toleration.Operator = corev1.TolerationOpEqual
	} else {
		toleration.Key = str
		toleration.Operator = corev1.TolerationOpExists
	}

	if toleration.Key == "" && toleration.Operator != corev1.TolerationOpExists {
		return corev1.Toleration{}, fmt.Errorf("invalid toleration %q: missing key", str)
	}
	return toleration, nil
}

// Set parses the provided string into the toleration list.
func (tl *TolerationList) Set(str string) error {
	if str == "" {
		return nil
	}
	for _, chunk := range strings.Split(str, ",") {
		toleration, err := parseToleration(strings.TrimSpace(chunk))
		if err != nil {
			return err
		}
		tl.Tolerations = append(tl.Tolerations, toleration)
	}
	return nil
}

// Type returns the tolerationList type.
func (tl TolerationList) Type() string {
	return "tolerationList"
}

// SortedClasses returns the classes handled by the list, sorted, to allow deterministic processing.
func (pl CapacitySharingPolicyList) SortedClasses() []string {
	classes := make([]string, 0, len(pl.Policies))
	for i := range pl.Policies {
		classes = append(classes, pl.Policies[i].Class)
	}
	sort.Strings(classes)
	return classes
}
