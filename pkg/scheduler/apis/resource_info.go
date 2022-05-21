/*
Copyright 2021-2022 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apis

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"math"
	"strings"
)

const (
	// GPUResourceName need to follow
	// https://github.com/NVIDIA/k8s-device-plugin/blob/66a35b71ac4b5cbfb04714678b548bd77e5ba719/server.go#L20
	GPUResourceName corev1.ResourceName = "nvidia.com/gpu"

	// minResource is used to qualify whether two quantities are equal
	minResource float64 = 0.1
)

// DimensionDefaultValue means default value for black resource dimension
type DimensionDefaultValue int

const (
	// Zero means resource dimension not defined will be treated as zero
	Zero DimensionDefaultValue = 0

	// Infinity means resource dimension not defined will be treated as infinity
	Infinity DimensionDefaultValue = -1
)

// Resource defines all the resource type.
type Resource struct {
	MilliCPU float64 // CPU usage
	Memory   float64 // Memory usage

	ScalarResources map[corev1.ResourceName]float64 // resources other than CPU and Memory

	// MaxTaskNum is only used by predicates; it should NOT
	// be accounted in other operators, e.g. Add.
	MaxTaskNum int
}

func EmptyResource() *Resource {
	return &Resource{}
}

func GetMinResource() float64 {
	return minResource
}

// IsScalarResourceName validates the resource for Extended, Hugepages, Native and AttachableVolume resources.
// NOTE: the func is copied from older version of `k8s.io/kubernetes/pkg/apis/core/v1/helper`.
func IsScalarResourceName(name corev1.ResourceName) bool {
	return v1helper.IsExtendedResourceName(name) || v1helper.IsHugePageResourceName(name) ||
		v1helper.IsPrefixedNativeResource(name) || v1helper.IsAttachableVolumeResourceName(name)
}

// SetScalar sets a resource by a scalar value of this resource.
func (r *Resource) SetScalar(name corev1.ResourceName, quantity float64) {
	if r.ScalarResources == nil {
		r.ScalarResources = map[corev1.ResourceName]float64{}
	}
	r.ScalarResources[name] = quantity
}

// AddScalar adds a resource by a scalar value of this resource.
func (r *Resource) AddScalar(name corev1.ResourceName, quantity float64) {
	r.SetScalar(name, r.ScalarResources[name]+quantity)
}

// NewResource creates a Resource instance.
func NewResource(resList corev1.ResourceList) *Resource {
	r := EmptyResource()
	for name, quantity := range resList {
		switch name {
		case corev1.ResourceCPU:
			r.MilliCPU += float64(quantity.MilliValue())
		case corev1.ResourceMemory:
			r.Memory += float64(quantity.Value())
		case corev1.ResourcePods:
			r.MaxTaskNum += int(quantity.Value())
		default:
			// NOTE: When converting this back to k8s resource, we need record the format as well as / 1000
			if IsScalarResourceName(name) {
				r.AddScalar(name, float64(quantity.MilliValue()))
			}
		}
	}
	return r
}

// ResFloat642Quantity transforms float64 to resource.Quantity.
func ResFloat642Quantity(resName corev1.ResourceName, resQuantity float64) resource.Quantity {
	var result *resource.Quantity
	switch resName {
	case corev1.ResourceCPU:
		result = resource.NewMilliQuantity(int64(resQuantity), resource.DecimalSI) // base is 10 (milli sec)
	default:
		result = resource.NewQuantity(int64(resQuantity), resource.BinarySI) // base is 2 (byte)
	}
	return *result
}

// ResQuantity2Float64 transforms resource.Quantity to float64.
func ResQuantity2Float64(resName corev1.ResourceName, quantity resource.Quantity) float64 {
	var result float64
	switch resName {
	case corev1.ResourceCPU:
		result = float64(quantity.MilliValue())
	default:
		result = float64(quantity.Value())
	}
	return result
}

// Clone deeply copies a Resource instance from r.
func (r *Resource) Clone() *Resource {
	clone := &Resource{
		MilliCPU:   r.MilliCPU,
		Memory:     r.Memory,
		MaxTaskNum: r.MaxTaskNum,
	}
	if r.ScalarResources != nil {
		clone.ScalarResources = make(map[corev1.ResourceName]float64)
		for k, v := range r.ScalarResources {
			clone.ScalarResources[k] = v
		}
	}
	return clone
}

func (r *Resource) String() string {
	str := fmt.Sprintf("cpu %0.2f, memory %0.2f", r.MilliCPU, r.Memory)
	for name, quantity := range r.ScalarResources {
		str = fmt.Sprintf("%s, %s %0.2f", str, name, quantity)
	}
	return str
}

// ResourceNameList collects resource names.
type ResourceNameList []corev1.ResourceName

// ResourceNames collects all used resource names in r.
func (r *Resource) ResourceNames() ResourceNameList {
	resNames := ResourceNameList{}

	if r.MilliCPU >= minResource {
		resNames = append(resNames, corev1.ResourceCPU)
	}
	if r.Memory >= minResource {
		resNames = append(resNames, corev1.ResourceMemory)
	}
	for name, quantity := range r.ScalarResources {
		if quantity >= minResource {
			resNames = append(resNames, name)
		}
	}

	return resNames
}

// Get returns the quantity of given resource name from r.
func (r *Resource) Get(resName corev1.ResourceName) float64 {
	switch resName {
	case corev1.ResourceCPU:
		return r.MilliCPU
	case corev1.ResourceMemory:
		return r.Memory
	default:
		if r.ScalarResources == nil {
			return 0
		}
		return r.ScalarResources[resName]
	}
}

// IsEmpty returns false if ANY kind of resource is NOT less than min value, otherwise returns true.
func (r *Resource) IsEmpty() bool {
	if !(r.MilliCPU < minResource && r.Memory < minResource) {
		return false
	}
	for _, quantity := range r.ScalarResources {
		if quantity >= minResource {
			return false
		}
	}
	return true
}

// IsZero returns false if the given kind of resource is not less than min value.
func (r *Resource) IsZero(resName corev1.ResourceName) bool {
	switch resName {
	case corev1.ResourceCPU:
		return r.MilliCPU < minResource
	case corev1.ResourceMemory:
		return r.Memory < minResource
	default:
		if r.ScalarResources == nil {
			return true
		}
		_, found := r.ScalarResources[resName]
		assert.Assertf(found, "unknown resource %s", resName)
		return r.ScalarResources[resName] < minResource
	}
}

// Add adds the resource quantity from rr to r for each kind of resource individually.
func (r *Resource) Add(rr *Resource) *Resource {
	r.MilliCPU += rr.MilliCPU
	r.Memory += rr.Memory

	for name, quantity := range rr.ScalarResources {
		if r.ScalarResources == nil {
			r.ScalarResources = map[corev1.ResourceName]float64{}
		}
		r.ScalarResources[name] += quantity
	}
	return r
}

// Equal returns true only on condition that values in all dimension are equal with each other for r and rr.
// Otherwise, it returns false.
// @param defaultValue "default value for resource dimension not defined in ScalarResources. Its value can only be one of 'Zero' and 'Infinity'".
func (r *Resource) Equal(rr *Resource, defaultVal DimensionDefaultValue) bool {
	equalFunc := func(l, r, diff float64) bool {
		return l == r || math.Abs(l-r) < diff
	}

	if !equalFunc(r.MilliCPU, rr.MilliCPU, minResource) || !equalFunc(r.Memory, rr.Memory, minResource) {
		return false
	}

	for name, leftVal := range r.ScalarResources {
		rightVal := rr.ScalarResources[name]
		if !equalFunc(leftVal, rightVal, minResource) {
			return false
		}
	}
	return true
}

// Less return true only on condition that all dimensions of resources in r are less than that of rr,
// Otherwise returns false.
// @param defaultValue "default value for resource dimension not defined in ScalarResources.
// Its value can only be one of 'Zero' and 'Infinity'".
func (r *Resource) Less(rr *Resource, defaultVal DimensionDefaultValue) bool {
	lessFunc := func(l, r float64) bool {
		return l < r
	}

	if !lessFunc(r.MilliCPU, rr.MilliCPU) {
		return false
	}
	if !lessFunc(r.Memory, rr.Memory) {
		return false
	}

	for name, leftVal := range r.ScalarResources {
		rightVal, ok := rr.ScalarResources[name]
		if !ok && defaultVal == Infinity {
			continue
		}
		if !lessFunc(leftVal, rightVal) {
			return false
		}
	}
	return true
}

// LessEqual returns true only on condition that all dimensions of resources in r are less than or equal with that of rr,
// Otherwise returns false.
// @param defaultValue "default value for resource dimension not defined in ScalarResources.
// Its value can only be one of 'Zero' and 'Infinity'".
func (r *Resource) LessEqual(rr *Resource, defaultVal DimensionDefaultValue) bool {
	lessEqualFunc := func(l, r, diff float64) bool {
		if l < r || math.Abs(l-r) < diff {
			return true
		}
		return false
	}

	if !lessEqualFunc(r.MilliCPU, rr.MilliCPU, minResource) {
		return false
	}
	if !lessEqualFunc(r.Memory, rr.Memory, minResource) {
		return false
	}

	for name, leftVal := range r.ScalarResources {
		rightVal, ok := rr.ScalarResources[name]
		if !ok && defaultVal == Infinity {
			continue
		}
		if !lessEqualFunc(leftVal, rightVal, minResource) {
			return false
		}
	}
	return true
}

// LessPartly returns true if there exists any dimension whose resource amount in r is less than that in rr.
// Otherwise, returns false.
// @param defaultValue "default value for resource dimension not defined in ScalarResources.
// Its value can only be one of 'Zero' and 'Infinity'".
func (r *Resource) LessPartly(rr *Resource, defaultVal DimensionDefaultValue) bool {
	lessFunc := func(l, r float64) bool {
		return l < r
	}

	if lessFunc(r.MilliCPU, rr.MilliCPU) || lessFunc(r.Memory, rr.Memory) {
		return true
	}

	for name, leftVal := range r.ScalarResources {
		rightVal, ok := rr.ScalarResources[name]
		if !ok && defaultVal == Infinity {
			return true
		}
		if lessFunc(leftVal, rightVal) {
			return true
		}
	}
	return false
}

// LessEqualPartly returns true if there exists any dimension whose resource amount in r is less than or equal with that in rr.
// Otherwise, returns false.
// @param defaultValue "default value for resource dimension not defined in ScalarResources.
// Its value can only be one of 'Zero' and 'Infinity'".
func (r *Resource) LessEqualPartly(rr *Resource, defaultVal DimensionDefaultValue) bool {
	lessEqualFunc := func(l, r, diff float64) bool {
		if l < r || math.Abs(l-r) < diff {
			return true
		}
		return false
	}

	if lessEqualFunc(r.MilliCPU, rr.MilliCPU, minResource) ||
		lessEqualFunc(r.Memory, rr.Memory, minResource) {
		return true
	}

	for name, leftVal := range r.ScalarResources {
		rightVal, ok := rr.ScalarResources[name]
		if !ok && defaultVal == Infinity {
			return true
		}
		if lessEqualFunc(leftVal, rightVal, minResource) {
			return true
		}
	}
	return false
}

func (r *Resource) sub(rr *Resource) *Resource {
	r.MilliCPU -= rr.MilliCPU
	r.Memory -= rr.Memory

	if r.ScalarResources == nil {
		return r
	}
	for name, quantity := range rr.ScalarResources {
		r.ScalarResources[name] -= quantity
	}
	return r
}

// Sub subs the quantity of each resource of rr from r. Check before sub.
func (r *Resource) Sub(rr *Resource) *Resource {
	assert.Assertf(rr.LessEqual(r, Zero), "resource is not sufficient to do operation: <%v> sub <%v>", r, rr)
	return r.sub(rr)
}

// Multi multiples a ratio and each resource in r.
func (r *Resource) Multi(ratio float64) *Resource {
	r.MilliCPU *= ratio
	r.Memory *= ratio
	for name, quantity := range r.ScalarResources {
		r.ScalarResources[name] = quantity * ratio
	}
	return r
}

// SetMaxResource compares the quantity of each Resource between r and rr and set the maximum to r.
func (r *Resource) SetMaxResource(rr *Resource) {
	if r == nil || rr == nil {
		return
	}

	if rr.MilliCPU > r.MilliCPU {
		r.MilliCPU = rr.MilliCPU
	}
	if rr.Memory > r.Memory {
		r.Memory = rr.Memory
	}

	for name, quantity := range rr.ScalarResources {
		if r.ScalarResources == nil {
			r.ScalarResources = map[corev1.ResourceName]float64{}
			for k, v := range rr.ScalarResources {
				r.ScalarResources[k] = v
			}
			return
		}
		_, ok := r.ScalarResources[name]
		if !ok || quantity > r.ScalarResources[name] {
			r.ScalarResources[name] = quantity
		}
	}
}

// FitDelta computes the delta between a resource object representing available and a resource object representing resources being requested.
// Any field that is less than 0 after the operation represents an "insufficient" resource.
func (r *Resource) FitDelta(rr *Resource) *Resource {
	if rr.MilliCPU > 0 {
		r.MilliCPU -= rr.MilliCPU + minResource
	}
	if rr.Memory > 0 {
		r.Memory -= rr.Memory + minResource
	}

	if r.ScalarResources == nil {
		r.ScalarResources = map[corev1.ResourceName]float64{}
	}

	for name, quantity := range rr.ScalarResources {
		if quantity > 0 {
			_, ok := r.ScalarResources[name]
			if !ok {
				r.ScalarResources[name] = 0
			}
			r.ScalarResources[name] -= quantity + minResource
		}
	}
	return r
}

// setDefaultValue sets default value for resource dimension of ScalarResource not defined in leftResource and rightResource.
// @param defaultVal is the default value ('Zero' or 'Infinity') for resource dimension not defined in ScalarResources.
// This operation called by Diff makes leftResource and rightResource have the same number of elements.
func (r *Resource) setDefaultValue(leftResource, rightResource *Resource, defaultVal DimensionDefaultValue) {
	if leftResource.ScalarResources == nil {
		leftResource.ScalarResources = map[corev1.ResourceName]float64{}
	}
	if rightResource.ScalarResources == nil {
		rightResource.ScalarResources = map[corev1.ResourceName]float64{}
	}

	for leftResName := range leftResource.ScalarResources {
		_, ok := rightResource.ScalarResources[leftResName]
		if !ok {
			if defaultVal == Zero {
				rightResource.ScalarResources[leftResName] = 0
			} else if defaultVal == Infinity {
				rightResource.ScalarResources[leftResName] = -1
			}
		}
	}

	for rightResName := range rightResource.ScalarResources {
		_, ok := leftResource.ScalarResources[rightResName]
		if !ok {
			if defaultVal == Zero {
				leftResource.ScalarResources[rightResName] = 0
			} else if defaultVal == Infinity {
				leftResource.ScalarResources[rightResName] = -1
			}
		}
	}
}

// Diff calculates the difference between two resource object.
// Note: if `defaultValue` equals `Infinity`, the difference between two values will be `Infinity`, marked as -1.
func (r *Resource) Diff(rr *Resource, defaultVal DimensionDefaultValue) (*Resource, *Resource) {
	leftRes, rightRes := r.Clone(), rr.Clone()
	increasedVal, decreasedVal := EmptyResource(), EmptyResource()
	r.setDefaultValue(leftRes, rightRes, defaultVal)

	if leftRes.MilliCPU > rightRes.MilliCPU {
		increasedVal.MilliCPU = leftRes.MilliCPU - rightRes.MilliCPU
	} else {
		decreasedVal.MilliCPU = rightRes.MilliCPU - leftRes.MilliCPU
	}

	if leftRes.Memory > rightRes.Memory {
		increasedVal.Memory = leftRes.Memory - rightRes.Memory
	} else {
		decreasedVal.Memory = rightRes.Memory - leftRes.Memory
	}

	increasedVal.ScalarResources = map[corev1.ResourceName]float64{}
	decreasedVal.ScalarResources = map[corev1.ResourceName]float64{}

	for leftResName, leftResQuantity := range leftRes.ScalarResources {
		rightResQuantity := rightRes.ScalarResources[leftResName]
		if leftResQuantity == -1 {
			increasedVal.ScalarResources[leftResName] = -1
			continue
		}
		if rightResQuantity == -1 {
			decreasedVal.ScalarResources[leftResName] = -1
			continue
		}
		if leftResQuantity > rightResQuantity {
			increasedVal.ScalarResources[leftResName] = leftResQuantity - rightResQuantity
		} else {
			decreasedVal.ScalarResources[leftResName] = rightResQuantity - leftResQuantity
		}
	}
	return increasedVal, decreasedVal
}

// MinDimensionResource is used to reset the resource dimension of r which is less than rr's.
// e.g r is <cpu 2000.00, memory 4047845376.00, hugepages-2Mi 0.00, hugepages-1Gi 0.00>, rr is <cpu 3000.00, memory 1000.00>,
// return <cpu 2000.00, memory 1000.00, hugepages-2Mi 0.00, hugepages-1Gi 0.00>.
// @param defaultVal is the default value ('Zero' or 'Infinity') for resource dimension not defined in ScalarResources.
func (r *Resource) MinDimensionResource(rr *Resource, defaultVal DimensionDefaultValue) *Resource {
	if r.MilliCPU > rr.MilliCPU {
		r.MilliCPU = rr.MilliCPU
	}
	if r.Memory > rr.Memory {
		r.Memory = rr.Memory
	}
	if r.ScalarResources == nil {
		return r
	}

	if rr.ScalarResources == nil {
		if defaultVal == Infinity {
			return r
		}
		for name := range r.ScalarResources {
			r.ScalarResources[name] = 0
		}
		return r
	}

	for name, quantity := range r.ScalarResources {
		rightQuantity, ok := rr.ScalarResources[name]
		if ok {
			r.ScalarResources[name] = math.Min(quantity, rightQuantity)
		} else {
			if defaultVal == Infinity {
				continue
			}
			r.ScalarResources[name] = 0
		}
	}

	return r
}

// ParseResourceList parses the given configuration map into a ResourceList.
func ParseResourceList(m map[string]string) (corev1.ResourceList, error) {
	if len(m) == 0 {
		return nil, nil
	}
	rl := corev1.ResourceList{}

	for k, v := range m {
		switch corev1.ResourceName(k) {
		// CPU, memory, local storage, and PID resources are supported.
		// TODO: maybe we could add more custom resource. For example, privacy data block.
		// 	In this case we need to define more CRDs.
		case corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage:
			quantity, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, err
			}
			if quantity.Sign() == -1 {
				return nil, fmt.Errorf("resource quantity for %q cannot be negative: %v", k, v)
			}
			rl[corev1.ResourceName(k)] = quantity
		default:
			return nil, fmt.Errorf("cannot reserve %q resource", k)
		}
	}
	return rl, nil
}

// Contains judges whether rrl is a subset of rl.
func (rl ResourceNameList) Contains(rrl ResourceNameList) bool {
	for _, rightResName := range ([]corev1.ResourceName)(rrl) {
		// for each rn in rrl, check whether rn is in rl
		isResExist := false
		for _, resName := range ([]corev1.ResourceName)(rl) {
			if rightResName == resName {
				isResExist = true
				break
			}
		}
		if !isResExist {
			return false
		}
	}
	return true
}

func IsCountQuota(name corev1.ResourceName) bool {
	return strings.HasPrefix(string(name), "count/")
}
