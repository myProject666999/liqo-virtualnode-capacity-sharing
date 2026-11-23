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
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	resourcehelper "k8s.io/component-helpers/resource"
	k8shelper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"

	"github.com/liqotech/liqo/pkg/consts"
	liqoutils "github.com/liqotech/liqo/pkg/utils"
)

// RatioBasis selects the quantity the sharing ratio is applied to.
type RatioBasis string

const (
	// RatioBasisAvailable applies the ratio to the capacity remaining after the requests of the provider-local
	// pods and the resources already granted to other ResourceSlices have been subtracted.
	// This is the default basis.
	RatioBasisAvailable RatioBasis = "Available"
	// RatioBasisTotal applies the ratio to the total schedulable capacity, subtracting the resources already
	// granted to other ResourceSlices afterwards.
	RatioBasisTotal RatioBasis = "Total"
)

const (
	// underlyingCapacityReason is the reason reported when the provider has no schedulable capacity left.
	underlyingCapacityReason = "the provider cluster has no schedulable capacity left"
	// noEligibleNodeReason is the reason reported when no provider node is eligible for the computation.
	noEligibleNodeReason = "the provider cluster has no eligible node"
)

var (
	ratZero  = new(big.Rat)
	ratMilli = big.NewRat(1, 1000)
	ratMebi  = big.NewRat(1024*1024, 1)
	bigTen   = big.NewInt(10)
	bigMebi  = big.NewInt(1024 * 1024)
	// helperLogger is the logger used by the helper functions invoked with a logger (e.g., taint toleration).
	helperLogger = klog.Background()
)

// CapacitySharingPolicy configures the capacity sharing for a ResourceSlice class.
type CapacitySharingPolicy struct {
	// Ratio is the integer percentage (1-100) of the provider capacity shared with the consumer.
	Ratio int
	// Basis selects whether the ratio is applied to the remaining or to the total capacity.
	// Empty defaults to RatioBasisAvailable.
	Basis RatioBasis
	// Tolerations are the tolerations used to decide whether a NoSchedule taint on a provider node is tolerable.
	// Nodes with untolerated NoSchedule taints are excluded from the computation.
	Tolerations []corev1.Toleration
	// Publish configures the thresholds used to avoid republishing negligible capacity changes.
	Publish PublishThresholds
}

// CapacitySharingInput is the input of the capacity sharing computation.
type CapacitySharingInput struct {
	// Nodes are the provider nodes.
	Nodes []corev1.Node
	// Pods are the pods running on the provider cluster.
	Pods []corev1.Pod
	// Granted is the amount of resources already granted to other consumers (i.e., through other accepted
	// ResourceSlices), which is subtracted from the schedulable capacity.
	Granted corev1.ResourceList
	// Usage is the amount of resources currently used by the consumer owning the ResourceSlice. It is used only to
	// detect overcommit, and it is never subtracted from the capacity, to avoid double-counting the granted amount.
	Usage corev1.ResourceList
	// Ratio is the integer percentage (1-100) of the capacity to share.
	Ratio int
	// Basis selects whether the ratio is applied to the remaining or to the total capacity.
	// Empty defaults to RatioBasisAvailable.
	Basis RatioBasis
	// Tolerations are the tolerations used to decide whether a NoSchedule taint on a provider node is tolerable.
	Tolerations []corev1.Toleration
}

// NodeExclusion describes why a provider node has been excluded from the capacity computation.
type NodeExclusion struct {
	NodeName string
	Reason   string
}

// NodeCapacity is the capacity computed for a single provider node.
type NodeCapacity struct {
	// NodeName is the stable identity of the node (i.e., its name), used to sort the entries deterministically.
	NodeName string
	// Schedulable is the node allocatable capacity minus the requests of the non-Liqo pods scheduled on it,
	// clamped to non-negative values.
	Schedulable corev1.ResourceList
	// GrantedShare is the portion of the resources granted to other consumers allocated to this node.
	GrantedShare corev1.ResourceList
	// Available is Schedulable minus GrantedShare, clamped to non-negative values.
	Available corev1.ResourceList
	// SharedAvailableBasis is the capacity shared with this node when the ratio is applied to the available capacity.
	SharedAvailableBasis corev1.ResourceList
	// SharedTotalBasis is the capacity shared with this node when the ratio is applied to the total capacity and the
	// granted share is subtracted afterwards.
	SharedTotalBasis corev1.ResourceList
	// Shared is the capacity shared with this node according to the selected basis.
	Shared corev1.ResourceList
}

// CapacitySharingResult is the output of the capacity sharing computation.
type CapacitySharingResult struct {
	// Resources is the capacity to grant to the consumer, computed according to the selected basis.
	Resources corev1.ResourceList
	// AvailableBasisResources is the capacity computed applying the ratio to the available capacity,
	// provided for comparison with TotalBasisResources on the same input data.
	AvailableBasisResources corev1.ResourceList
	// TotalBasisResources is the capacity computed applying the ratio to the total capacity, provided for
	// comparison with AvailableBasisResources on the same input data.
	TotalBasisResources corev1.ResourceList
	// Basis is the basis used to populate Resources.
	Basis RatioBasis
	// Schedulable is the aggregate schedulable capacity of the eligible nodes, before subtracting the grants.
	Schedulable corev1.ResourceList
	// Granted is the amount of resources actually subtracted from the schedulable capacity.
	Granted corev1.ResourceList
	// Ready reports whether the advertised capacity contains a positive amount of both CPU and memory,
	// coherently with the consumer-side readiness check.
	Ready bool
	// NotReadyReason explains why the advertised capacity is not ready, if Ready is false.
	NotReadyReason string
	// Overcommitted reports whether the advertised capacity is lower than the resources currently used.
	Overcommitted bool
	// OvercommitReason explains which resources are advertised below their current usage, if any.
	OvercommitReason string
	// PerNode contains the per-node capacity, sorted by node name.
	PerNode []NodeCapacity
	// ExcludedNodes contains the nodes excluded from the computation, sorted by node name.
	ExcludedNodes []NodeExclusion
	// SingleNodeMax contains, for the extended resources (e.g., GPUs), the maximum amount that a single node can
	// offer. A single pod cannot consume more than this amount, even if the aggregate is larger.
	SingleNodeMax corev1.ResourceList
	// FragmentationReason explains the extended resources for which the aggregate capacity is larger than the
	// amount a single node can offer.
	FragmentationReason string
}

// isLiqoManagedPod returns whether the pod has been created by Liqo from a ShadowPod.
func isLiqoManagedPod(pod *corev1.Pod) bool {
	return pod.Labels[consts.ManagedByLabelKey] == consts.ManagedByShadowPodValue
}

// isTerminalPod returns whether the pod has reached a terminal phase, hence it does not consume node resources.
func isTerminalPod(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// isExtendedResource returns whether the resource is an extended resource (e.g., nvidia.com/gpu).
func isExtendedResource(name corev1.ResourceName) bool {
	if strings.HasPrefix(string(name), corev1.ResourceHugePagesPrefix) {
		return false
	}
	return strings.Contains(string(name), "/")
}

// isMemoryLikeResource returns whether the resource is measured in bytes and truncated to MiB.
func isMemoryLikeResource(name corev1.ResourceName) bool {
	return name == corev1.ResourceMemory || name == corev1.ResourceEphemeralStorage ||
		strings.HasPrefix(string(name), corev1.ResourceHugePagesPrefix)
}

// unitFor returns the unit the given resource is truncated to: millicores for the CPU, MiB for the memory-like
// resources, and the integer unit for the pod count and the extended resources.
func unitFor(name corev1.ResourceName) *big.Rat {
	switch {
	case name == corev1.ResourceCPU:
		return ratMilli
	case isMemoryLikeResource(name):
		return ratMebi
	default:
		return big.NewRat(1, 1)
	}
}

// quantityToRat converts a resource quantity into an exact rational number.
// The quantity is represented as an inf.Dec, whose value is unscaled * 10^(-scale).
func quantityToRat(quantity resource.Quantity) *big.Rat {
	dec := quantity.AsDec()
	num := new(big.Int).Set(dec.UnscaledBig())
	den := big.NewInt(1)
	scale := int64(dec.Scale())
	if scale > 0 {
		den = new(big.Int).Exp(bigTen, big.NewInt(scale), nil)
	} else if scale < 0 {
		num.Mul(num, new(big.Int).Exp(bigTen, big.NewInt(-scale), nil))
	}
	return new(big.Rat).SetFrac(num, den)
}

// floorToUnit truncates the given rational value to an integer multiple of the unit, rounding towards minus infinity.
func floorToUnit(value, unit *big.Rat) *big.Int {
	scaled := new(big.Rat).Quo(value, unit)
	quotient, remainder := new(big.Int).QuoRem(scaled.Num(), scaled.Denom(), new(big.Int))
	if remainder.Sign() < 0 {
		quotient.Sub(quotient, big.NewInt(1))
	}
	return quotient
}

// multiplyByRatio returns value * ratio / 100.
func multiplyByRatio(value *big.Rat, ratio int) *big.Rat {
	return new(big.Rat).Mul(value, big.NewRat(int64(ratio), 100))
}

// clampInt64 clamps a big integer into the int64 range.
func clampInt64(value *big.Int) int64 {
	if value.Cmp(big.NewInt(math.MaxInt64)) > 0 {
		return math.MaxInt64
	}
	if value.Cmp(big.NewInt(math.MinInt64)) < 0 {
		return math.MinInt64
	}
	return value.Int64()
}

// quantityFromIntegerUnit converts an integer multiple of the resource unit into a resource quantity.
func quantityFromIntegerUnit(name corev1.ResourceName, value *big.Int) resource.Quantity {
	switch {
	case name == corev1.ResourceCPU:
		return *resource.NewMilliQuantity(clampInt64(value), resource.DecimalSI)
	case isMemoryLikeResource(name):
		return *resource.NewQuantity(clampInt64(new(big.Int).Mul(value, bigMebi)), resource.BinarySI)
	default:
		return *resource.NewQuantity(clampInt64(value), resource.DecimalSI)
	}
}

// truncateQuantity truncates the given rational value to the unit of the resource, clamping negative values to zero.
func truncateQuantity(name corev1.ResourceName, value *big.Rat) resource.Quantity {
	if value.Sign() < 0 {
		value = new(big.Rat)
	}
	return quantityFromIntegerUnit(name, floorToUnit(value, unitFor(name)))
}

// nodeExclusionReason returns the reason why the node must be excluded from the capacity computation, or an empty
// string if the node is eligible. Tolerations are the explicit input used to accept NoSchedule taints.
func nodeExclusionReason(node *corev1.Node, tolerations []corev1.Toleration) string {
	if node.Spec.Unschedulable {
		return "node is unschedulable (cordoned)"
	}
	if !liqoutils.IsNodeReady(node) {
		return "node is not ready"
	}
	for i := range node.Spec.Taints {
		taint := &node.Spec.Taints[i]
		if taint.Effect != corev1.TaintEffectNoSchedule {
			continue
		}
		if !k8shelper.TolerationsTolerateTaint(helperLogger, tolerations, taint, false) {
			return fmt.Sprintf("node has an untolerated NoSchedule taint %q", taint.Key)
		}
	}
	return ""
}

// podRequests returns the resource requests of the pod, including init containers, sidecars and overhead.
func podRequests(pod *corev1.Pod) corev1.ResourceList {
	return resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
}

// nodeSchedulableResources computes the schedulable capacity of the node, i.e., its allocatable capacity minus the
// resource requests of the non-Liqo pods scheduled on it, clamped to non-negative values.
// Liqo-managed pods are not subtracted, since the resources they consume are accounted for through the granted
// resources, thus avoiding a double deduction.
func nodeSchedulableResources(node *corev1.Node, pods []corev1.Pod) corev1.ResourceList {
	schedulable := corev1.ResourceList{}
	for name, quantity := range node.Status.Allocatable {
		schedulable[name] = quantity.DeepCopy()
	}

	var podCount int64
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName != node.GetName() || isLiqoManagedPod(pod) || isTerminalPod(pod) {
			continue
		}
		podCount++

		for name, requested := range podRequests(pod) {
			available, ok := schedulable[name]
			if !ok {
				// Only resources advertised by the node are considered.
				continue
			}
			available.Sub(requested)
			schedulable[name] = available
		}
	}

	if podsQuantity, ok := schedulable[corev1.ResourcePods]; ok {
		podsQuantity.Sub(*resource.NewQuantity(podCount, resource.DecimalSI))
		schedulable[corev1.ResourcePods] = podsQuantity
	}

	for name, quantity := range schedulable {
		if quantity.Sign() < 0 {
			schedulable[name] = *resource.NewQuantity(0, quantity.Format)
		}
	}
	return schedulable
}

// subtractFloor returns max(0, minuend - subtrahend) for the given resource.
func subtractFloor(minuend, subtrahend resource.Quantity) resource.Quantity {
	value := minuend.DeepCopy()
	value.Sub(subtrahend)
	if value.Sign() < 0 {
		return *resource.NewQuantity(0, minuend.Format)
	}
	return value
}

// sumQuantities sums the quantities with the given name across the resource lists.
func sumQuantities(name corev1.ResourceName, lists []corev1.ResourceList) resource.Quantity {
	total := *resource.NewQuantity(0, resource.DecimalSI)
	for _, list := range lists {
		if quantity, ok := list[name]; ok {
			total.Add(quantity)
		}
	}
	return total
}

// sortedResourceNames returns the union of the keys of the given resource lists, sorted.
func sortedResourceNames(lists ...corev1.ResourceList) []corev1.ResourceName {
	seen := map[corev1.ResourceName]struct{}{}
	for _, list := range lists {
		for name := range list {
			seen[name] = struct{}{}
		}
	}
	names := make([]corev1.ResourceName, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
	return names
}

// allocateGrantedShare distributes the granted resources across the nodes, in stable node-name order.
// The whole granted amount is allocated at most once: each node first satisfies the grant with its own schedulable
// capacity, and only the remaining grant is passed to the next node. The per-node shares therefore sum to at most
// the granted amount, which is what makes the difference between the per-node output and the aggregate one stem
// only from the allocation policy and from the per-node truncation.
func allocateGrantedShare(schedulable []corev1.ResourceList, order []int, resourceName corev1.ResourceName,
	granted resource.Quantity) map[int]resource.Quantity {
	shares := map[int]resource.Quantity{}
	remaining := granted.DeepCopy()
	for _, idx := range order {
		if remaining.Sign() <= 0 {
			break
		}
		available, ok := schedulable[idx][resourceName]
		if !ok || available.Sign() <= 0 {
			continue
		}
		share := available.DeepCopy()
		if remaining.Cmp(available) < 0 {
			share = remaining.DeepCopy()
		}
		shares[idx] = share
		remaining.Sub(share)
	}
	return shares
}

// validateRatio validates the sharing ratio, rejecting zero and out-of-range values.
func validateRatio(ratio int) error {
	if ratio < 1 || ratio > 100 {
		return fmt.Errorf("invalid capacity sharing ratio %d: it must be in the range [1, 100]", ratio)
	}
	return nil
}

// validateBasis validates the ratio basis, defaulting empty to RatioBasisAvailable.
func validateBasis(basis RatioBasis) (RatioBasis, error) {
	switch basis {
	case "", RatioBasisAvailable:
		return RatioBasisAvailable, nil
	case RatioBasisTotal:
		return RatioBasisTotal, nil
	default:
		return "", fmt.Errorf("invalid capacity sharing basis %q: it must be either %q or %q",
			basis, RatioBasisAvailable, RatioBasisTotal)
	}
}

// ComputeSharedCapacity computes the provider capacity to share with a consumer, given the provider nodes and pods,
// the resources already granted to other consumers, the current usage of the consumer and the sharing ratio.
// The function is pure: it does not mutate its input, it does not perform any I/O and it is deterministic.
func ComputeSharedCapacity(input CapacitySharingInput) (*CapacitySharingResult, error) {
	if err := validateRatio(input.Ratio); err != nil {
		return nil, err
	}
	basis, err := validateBasis(input.Basis)
	if err != nil {
		return nil, err
	}

	// Select the eligible nodes, preserving a deterministic order.
	nodes := make([]corev1.Node, len(input.Nodes))
	for i := range input.Nodes {
		nodes[i] = *input.Nodes[i].DeepCopy()
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].GetName() < nodes[j].GetName() })

	eligible := make([]*corev1.Node, 0, len(nodes))
	excluded := make([]NodeExclusion, 0)
	for i := range nodes {
		if reason := nodeExclusionReason(&nodes[i], input.Tolerations); reason != "" {
			excluded = append(excluded, NodeExclusion{NodeName: nodes[i].GetName(), Reason: reason})
			continue
		}
		eligible = append(eligible, &nodes[i])
	}

	schedulable := make([]corev1.ResourceList, len(eligible))
	for i := range eligible {
		schedulable[i] = nodeSchedulableResources(eligible[i], input.Pods)
	}

	// Only resources advertised by at least one eligible node are considered.
	names := sortedResourceNames(schedulable...)

	aggregateSchedulable := corev1.ResourceList{}
	for _, name := range names {
		aggregateSchedulable[name] = sumQuantities(name, schedulable)
	}

	// Distribute the granted resources across the nodes, in stable order.
	order := make([]int, len(eligible))
	for i := range order {
		order[i] = i
	}
	grantedShares := make([]corev1.ResourceList, len(eligible))
	for i := range grantedShares {
		grantedShares[i] = corev1.ResourceList{}
	}
	effectiveGranted := corev1.ResourceList{}
	for _, name := range names {
		grantQuantity, ok := input.Granted[name]
		if !ok {
			continue
		}
		shares := allocateGrantedShare(schedulable, order, name, grantQuantity)
		total := *resource.NewQuantity(0, resource.DecimalSI)
		for idx, share := range shares {
			grantedShares[idx][name] = share
			total.Add(share)
		}
		if total.Sign() > 0 {
			effectiveGranted[name] = total
		}
	}

	aggregateAvailable := corev1.ResourceList{}
	for _, name := range names {
		aggregateAvailable[name] = subtractFloor(aggregateSchedulable[name], effectiveGranted[name])
	}

	// Compute the aggregate capacity for both bases.
	// The total basis truncates the final value, so that the advertised amount stays on the resource unit boundary.
	availableBasis := corev1.ResourceList{}
	totalBasis := corev1.ResourceList{}
	for _, name := range names {
		availableBasis[name] = truncateQuantity(name, multiplyByRatio(quantityToRat(aggregateAvailable[name]), input.Ratio))

		scaledTotal := multiplyByRatio(quantityToRat(aggregateSchedulable[name]), input.Ratio)
		scaledTotal.Sub(scaledTotal, quantityToRat(effectiveGranted[name]))
		totalBasis[name] = truncateQuantity(name, scaledTotal)
	}

	// Compute the per-node capacity for both bases.
	perNode := make([]NodeCapacity, len(eligible))
	for i := range eligible {
		available := corev1.ResourceList{}
		for _, name := range names {
			available[name] = subtractFloor(schedulable[i][name], grantedShares[i][name])
		}

		sharedAvailable := corev1.ResourceList{}
		sharedTotal := corev1.ResourceList{}
		for _, name := range names {
			sharedAvailable[name] = truncateQuantity(name, multiplyByRatio(quantityToRat(available[name]), input.Ratio))

			scaledTotal := multiplyByRatio(quantityToRat(schedulable[i][name]), input.Ratio)
			scaledTotal.Sub(scaledTotal, quantityToRat(grantedShares[i][name]))
			sharedTotal[name] = truncateQuantity(name, scaledTotal)
		}

		perNode[i] = NodeCapacity{
			NodeName:             eligible[i].GetName(),
			Schedulable:          schedulable[i],
			GrantedShare:         grantedShares[i],
			Available:            available,
			SharedAvailableBasis: sharedAvailable,
			SharedTotalBasis:     sharedTotal,
		}
		if basis == RatioBasisTotal {
			perNode[i].Shared = sharedTotal
		} else {
			perNode[i].Shared = sharedAvailable
		}
	}

	result := &CapacitySharingResult{
		AvailableBasisResources: availableBasis,
		TotalBasisResources:     totalBasis,
		Basis:                   basis,
		Schedulable:             aggregateSchedulable,
		Granted:                 effectiveGranted,
		PerNode:                 perNode,
		ExcludedNodes:           excluded,
	}
	if basis == RatioBasisTotal {
		result.Resources = totalBasis
	} else {
		result.Resources = availableBasis
	}

	result.SingleNodeMax = extendedResourcesSingleNodeMax(names, perNode, result.Resources)
	result.FragmentationReason = fragmentationReason(names, perNode, result.Resources, result.SingleNodeMax)

	result.Ready = liqoutils.AreResourcesReady(result.Resources)
	if !result.Ready {
		result.NotReadyReason = notReadyReason(result, len(eligible))
	}

	result.Overcommitted, result.OvercommitReason = overcommitStatus(result.Resources, input.Usage)

	return result, nil
}

// extendedResourcesSingleNodeMax returns, for every extended resource, the maximum amount a single node can offer.
func extendedResourcesSingleNodeMax(names []corev1.ResourceName, perNode []NodeCapacity,
	aggregate corev1.ResourceList) corev1.ResourceList {
	singleNodeMax := corev1.ResourceList{}
	for _, name := range names {
		if !isExtendedResource(name) {
			continue
		}
		maximum := *resource.NewQuantity(0, resource.DecimalSI)
		for i := range perNode {
			if quantity, ok := perNode[i].Shared[name]; ok && quantity.Cmp(maximum) > 0 {
				maximum = quantity.DeepCopy()
			}
		}
		if aggregateQuantity, ok := aggregate[name]; ok && aggregateQuantity.Sign() > 0 {
			singleNodeMax[name] = maximum
		}
	}
	return singleNodeMax
}

// fragmentationReason explains the extended resources whose aggregate capacity is larger than the amount a single
// node can offer, hence a single pod cannot consume the whole aggregate.
func fragmentationReason(names []corev1.ResourceName, perNode []NodeCapacity,
	aggregate, singleNodeMax corev1.ResourceList) string {
	var fragments []string
	for _, name := range names {
		if !isExtendedResource(name) {
			continue
		}
		aggregateQuantity, ok := aggregate[name]
		if !ok || aggregateQuantity.Sign() <= 0 {
			continue
		}
		if singleQuantity, ok := singleNodeMax[name]; ok && aggregateQuantity.Cmp(singleQuantity) > 0 {
			fragments = append(fragments, fmt.Sprintf(
				"%s can be offered up to %s by a single node, while the aggregate is %s",
				name, singleQuantity.String(), aggregateQuantity.String()))
		}
	}
	return strings.Join(fragments, "; ")
}

// notReadyReason explains why the advertised capacity has no positive CPU or memory.
func notReadyReason(result *CapacitySharingResult, eligibleNodes int) string {
	var missing []string
	cpu := result.Resources.Cpu()
	if cpu == nil || cpu.CmpInt64(0) <= 0 {
		missing = append(missing, "CPU")
	}
	memory := result.Resources.Memory()
	if memory == nil || memory.CmpInt64(0) <= 0 {
		missing = append(missing, "memory")
	}

	reason := fmt.Sprintf("the provider can share no %s capacity", strings.Join(missing, " or "))
	switch {
	case eligibleNodes == 0 && len(result.ExcludedNodes) > 0:
		return fmt.Sprintf("%s: %s (%s: %s)", reason, noEligibleNodeReason,
			result.ExcludedNodes[0].NodeName, result.ExcludedNodes[0].Reason)
	case eligibleNodes == 0:
		return fmt.Sprintf("%s: %s", reason, noEligibleNodeReason)
	case result.Schedulable.Cpu().CmpInt64(0) <= 0 && result.Schedulable.Memory().CmpInt64(0) <= 0:
		return fmt.Sprintf("%s: %s", reason, underlyingCapacityReason)
	case len(result.Granted) > 0:
		return fmt.Sprintf("%s: the schedulable capacity is exhausted by the resources already granted "+
			"to other ResourceSlices, by the pods already running on the provider or by the sharing ratio", reason)
	default:
		return fmt.Sprintf("%s: the schedulable capacity is too small for the sharing ratio", reason)
	}
}

// overcommitStatus compares the advertised capacity with the resources currently used by the consumer.
func overcommitStatus(advertised, usage corev1.ResourceList) (bool, string) {
	var overcommitted []string
	for _, name := range sortedResourceNames(usage) {
		used := usage[name]
		available, ok := advertised[name]
		if ok && available.Cmp(used) >= 0 {
			continue
		}
		if !ok {
			available = *resource.NewQuantity(0, used.Format)
		}
		overcommitted = append(overcommitted, fmt.Sprintf("%s (%s advertised, %s used)",
			name, available.String(), used.String()))
	}
	if len(overcommitted) == 0 {
		return false, ""
	}
	return true, fmt.Sprintf("the advertised capacity is below the current usage for: %s; "+
		"existing pods are not evicted", strings.Join(overcommitted, ", "))
}

// PublishThresholds configures when a computed capacity change must be published.
// The caller must compare the candidate capacity against the last published one, so that a sequence of
// sub-threshold changes accumulates instead of never being published.
type PublishThresholds struct {
	// Absolute is the minimum absolute change, per resource, triggering a publication.
	Absolute corev1.ResourceList
	// Relative is the minimum relative change (e.g., 0.10 for 10%) triggering a publication.
	Relative float64
}

// DefaultPublishThresholds returns the default publication thresholds, which publish any capacity change.
func DefaultPublishThresholds() PublishThresholds {
	return PublishThresholds{Relative: 0}
}

// thresholdFor returns the minimum change triggering a publication for the given resource.
// When the previous value is zero, the relative threshold does not apply, so that a resource appearing from zero
// always triggers a publication (zero baseline); the absolute threshold still applies, if configured.
func (t PublishThresholds) thresholdFor(name corev1.ResourceName, previous resource.Quantity) *big.Rat {
	threshold := ratZero
	if absolute, ok := t.Absolute[name]; ok {
		threshold = quantityToRat(absolute)
	}
	if t.Relative > 0 && previous.Sign() > 0 {
		relative := new(big.Rat).Mul(quantityToRat(previous), new(big.Rat).SetFloat64(t.Relative))
		if relative.Cmp(threshold) > 0 {
			threshold = relative
		}
	}
	return threshold
}

// ShouldPublish returns whether the candidate capacity must be published instead of the previous one, together
// with the list of the resources whose change exceeds the thresholds.
// A resource is considered changed only if it differs from the previous value; the zero baseline is defined so
// that a resource appearing from zero always triggers a publication, while equal lists never do.
func (t PublishThresholds) ShouldPublish(previous, candidate corev1.ResourceList) (bool, []string) {
	var changed []string
	for _, name := range sortedResourceNames(previous, candidate) {
		before := previous[name]
		after := candidate[name]
		if before.Cmp(after) == 0 {
			continue
		}

		delta := quantityToRat(after)
		delta.Sub(delta, quantityToRat(before))
		delta.Abs(delta)

		if delta.Cmp(t.thresholdFor(name, before)) < 0 {
			continue
		}
		changed = append(changed, string(name))
	}
	return len(changed) > 0, changed
}
