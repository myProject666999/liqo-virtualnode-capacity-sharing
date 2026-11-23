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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	authv1beta1 "github.com/liqotech/liqo/apis/authentication/v1beta1"
	liqov1beta1 "github.com/liqotech/liqo/apis/core/v1beta1"
	"github.com/liqotech/liqo/pkg/liqo-controller-manager/authentication"
	liqoutils "github.com/liqotech/liqo/pkg/utils"
	"github.com/liqotech/liqo/pkg/virtualKubelet/forge"
)

const (
	// resourcesAcceptedReason is the reason of the Resources condition on the legacy accept-as-requested path.
	resourcesAcceptedReason = "ResourceSliceResourcesAccepted"
	// resourcesScarceReason is the reason of the Resources condition when the shared capacity is not ready.
	resourcesScarceReason = "ResourceSliceResourcesNotReady"
	// resourcesOvercommittedReason is the reason of the Resources condition when the shared capacity is below
	// the current usage.
	resourcesOvercommittedReason = "ResourceSliceResourcesOvercommitted"
)

// capacitySharingPolicyFor returns the capacity sharing policy configured for the given class, if any.
// A nil policy means that the class relies on the legacy accept-as-requested behavior.
func (r *RemoteResourceSliceReconciler) capacitySharingPolicyFor(
	class authv1beta1.ResourceSliceClass) (*CapacitySharingPolicy, bool) {
	if r.sliceStatusOptions == nil {
		return nil, false
	}
	policy, ok := r.sliceStatusOptions.CapacitySharingPolicies[class]
	if !ok {
		return nil, false
	}
	return &policy, true
}

// handleCapacitySharing computes the provider capacity to share with the consumer of the given ResourceSlice,
// according to the policy configured for its class, and reports it in the ResourceSlice status.
// The capacity is written in status.resources, so that the existing Quota and VirtualNode forging paths keep
// working unchanged. This function performs the pure capacity conversion only: it does not implement any
// authorization or approval workflow.
func (r *RemoteResourceSliceReconciler) handleCapacitySharing(ctx context.Context, resourceSlice *authv1beta1.ResourceSlice,
	policy *CapacitySharingPolicy) error {
	input, err := r.capacitySharingInput(ctx, resourceSlice, policy)
	if err != nil {
		return err
	}

	result, err := ComputeSharedCapacity(*input)
	if err != nil {
		// An invalid policy is a configuration error: deny the resources with the underlying reason, rather than
		// advertising an arbitrary capacity.
		klog.Errorf("Unable to compute the shared capacity for the ResourceSlice %q: %v",
			client.ObjectKeyFromObject(resourceSlice), err)
		denyResourcesWithReason(resourceSlice, r.eventRecorder, "InvalidCapacitySharingPolicy", err.Error())
		return nil
	}

	// Apply the publication thresholds against the last published capacity. A sub-threshold change is not
	// published, so that frequent micro-fluctuations do not trigger a cascade of VirtualNode updates.
	previous := resourceSlice.Status.Resources.DeepCopy()
	publish, changed := policy.Publish.ShouldPublish(previous, result.Resources)
	if !publish && previous != nil {
		klog.V(5).Infof("ResourceSlice %q capacity change below the publication thresholds (resources: %v), keeping %v",
			client.ObjectKeyFromObject(resourceSlice), changed, previous)
		// The derived flags must describe the capacity actually published, not the suppressed candidate.
		result.Resources = previous
		result.Ready = liqoutils.AreResourcesReady(previous)
		result.NotReadyReason = ""
		if !result.Ready {
			result.NotReadyReason = notReadyReason(result, len(result.PerNode))
		}
		result.Overcommitted, result.OvercommitReason = overcommitStatus(previous, input.Usage)
	}

	// Reuse the same status fields as the legacy path, so that the storage, ingress and load balancer classes
	// are advertised exactly as before.
	if err = r.applySliceStatusClasses(ctx, resourceSlice); err != nil {
		return err
	}

	resourceSlice.Status.Resources = result.Resources

	reason, message := capacitySharingCondition(result)
	switch {
	case reason == resourcesScarceReason:
		r.eventRecorder.Event(resourceSlice, corev1.EventTypeWarning, reason, message)
	case reason == resourcesOvercommittedReason:
		r.eventRecorder.Event(resourceSlice, corev1.EventTypeWarning, reason, message)
	}
	acceptResourcesWithReason(resourceSlice, r.eventRecorder, reason, message)

	klog.V(4).Infof("ResourceSlice %q capacity sharing computed (class %q, ratio %d, basis %q): resources %v, "+
		"per-node %d, excluded %d", client.ObjectKeyFromObject(resourceSlice), resourceSlice.Status.Class,
		policy.Ratio, result.Basis, result.Resources, len(result.PerNode), len(result.ExcludedNodes))
	return nil
}

// applySliceStatusClasses populates the status fields describing the offered storage, ingress and load balancer
// classes, reusing the same helpers as the legacy path.
func (r *RemoteResourceSliceReconciler) applySliceStatusClasses(ctx context.Context,
	resourceSlice *authv1beta1.ResourceSlice) error {
	storageClasses, err := getStorageClasses(ctx, r.Client, r.sliceStatusOptions)
	if err != nil {
		klog.Errorf("Unable to get the StorageClasses for the ResourceSlice %q: %s",
			client.ObjectKeyFromObject(resourceSlice), err)
		r.eventRecorder.Event(resourceSlice, corev1.EventTypeWarning, "StorageClassesFailed", err.Error())
		return err
	}
	resourceSlice.Status.StorageClasses = storageClasses
	resourceSlice.Status.IngressClasses = getIngressClasses(r.sliceStatusOptions)
	resourceSlice.Status.LoadBalancerClasses = getLoadBalancerClasses(r.sliceStatusOptions)
	resourceSlice.Status.NodeLabels = getNodeLabels(r.sliceStatusOptions)
	return nil
}

// capacitySharingCondition returns the reason and message describing the computed capacity.
// The fragmentation limitations of the extended resources (e.g., GPUs) are always reported in the message, since
// an aggregate amount larger than what a single node can offer cannot be consumed by a single pod.
func capacitySharingCondition(result *CapacitySharingResult) (reason, message string) {
	var base string
	switch {
	case !result.Ready:
		reason, base = resourcesScarceReason, result.NotReadyReason
	case result.Overcommitted:
		reason, base = resourcesOvercommittedReason, result.OvercommitReason
	default:
		reason, base = resourcesAcceptedReason, "ResourceSlice resources accepted"
	}
	if result.FragmentationReason != "" {
		base = fmt.Sprintf("%s; %s", base, result.FragmentationReason)
	}
	return reason, base
}

// capacitySharingInput gathers the provider-side data needed by the capacity sharing computation.
func (r *RemoteResourceSliceReconciler) capacitySharingInput(ctx context.Context,
	resourceSlice *authv1beta1.ResourceSlice, policy *CapacitySharingPolicy) (*CapacitySharingInput, error) {
	nodes, err := r.providerNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to list the provider nodes: %w", err)
	}

	pods, err := r.providerPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to list the provider pods: %w", err)
	}

	granted, err := r.grantedResources(ctx, resourceSlice)
	if err != nil {
		return nil, fmt.Errorf("unable to compute the resources already granted: %w", err)
	}

	return &CapacitySharingInput{
		Nodes:       nodes,
		Pods:        pods,
		Granted:     granted,
		Usage:       usageResources(pods, resourceSlice.Spec.ConsumerClusterID),
		Ratio:       policy.Ratio,
		Basis:       policy.Basis,
		Tolerations: policy.Tolerations,
	}, nil
}

// providerNodes returns the provider nodes, excluding the Liqo virtual nodes representing remote clusters.
func (r *RemoteResourceSliceReconciler) providerNodes(ctx context.Context) ([]corev1.Node, error) {
	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		return nil, err
	}

	nodes := make([]corev1.Node, 0, len(nodeList.Items))
	for i := range nodeList.Items {
		if liqoutils.IsVirtualNode(&nodeList.Items[i]) {
			continue
		}
		nodes = append(nodes, nodeList.Items[i])
	}
	return nodes, nil
}

// providerPods returns the pods running on the provider cluster.
func (r *RemoteResourceSliceReconciler) providerPods(ctx context.Context) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// grantedResources returns the resources already granted to other consumers, i.e., the capacity advertised by the
// other accepted ResourceSlices. The ResourceSlice being reconciled is excluded, so that its own previously
// published capacity is not subtracted from itself.
func (r *RemoteResourceSliceReconciler) grantedResources(ctx context.Context,
	resourceSlice *authv1beta1.ResourceSlice) (corev1.ResourceList, error) {
	var sliceList authv1beta1.ResourceSliceList
	if err := r.List(ctx, &sliceList); err != nil {
		return nil, err
	}

	self := client.ObjectKeyFromObject(resourceSlice)
	granted := corev1.ResourceList{}
	for i := range sliceList.Items {
		other := &sliceList.Items[i]
		if client.ObjectKeyFromObject(other) == self {
			continue
		}
		cond := authentication.GetCondition(other, authv1beta1.ResourceSliceConditionTypeResources)
		if cond == nil || cond.Status != authv1beta1.ResourceSliceConditionAccepted {
			continue
		}
		for name, quantity := range other.Status.Resources {
			if current, ok := granted[name]; ok {
				current.Add(quantity)
				granted[name] = current
				continue
			}
			granted[name] = quantity.DeepCopy()
		}
	}
	return granted, nil
}

// usageResources returns the resources currently used on the provider by the given consumer, i.e., the sum of the
// requests of the Liqo-managed pods originating from its cluster. It is used only to detect overcommit.
func usageResources(pods []corev1.Pod, consumerClusterID *liqov1beta1.ClusterID) corev1.ResourceList {
	if consumerClusterID == nil {
		return corev1.ResourceList{}
	}

	usage := corev1.ResourceList{}
	for i := range pods {
		pod := &pods[i]
		if !isLiqoManagedPod(pod) || pod.Labels[forge.LiqoOriginClusterIDKey] != string(*consumerClusterID) {
			continue
		}
		if isTerminalPod(pod) {
			continue
		}
		for name, quantity := range podRequests(pod) {
			if current, ok := usage[name]; ok {
				current.Add(quantity)
				usage[name] = current
				continue
			}
			usage[name] = quantity.DeepCopy()
		}
	}
	return usage
}
