// Package v1alpha1 contains the API schema for the clink operator's custom
// resources. Group clink.dev, version v1alpha1.
// +kubebuilder:object:generate=true
// +groupName=clink.dev
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group/version for the clink operator API.
	GroupVersion = schema.GroupVersion{Group: "clink.dev", Version: "v1alpha1"}

	// SchemeBuilder registers the API types with a runtime scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
