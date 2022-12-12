/*
Copyright 2020 Red Hat OpenShift Container Storage.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// FulfillStorageClassClaimSpec defines the desired state of FulfillStorageClassClaim
type FulfillStorageClassClaimSpec struct {
	//+kubebuilder:validation:Enum=blockpool;sharedfilesystem
	Type             string `json:"type"`
	EncryptionMethod string `json:"encryptionMethod,omitempty"`
	StorageProfile   string `json:"storageProfile,omitempty"`
}

type fulfillstorageClassClaimState string

const (
	// FulfillStorageClassClaimInitializing represents Initializing state of FulfillStorageClassClaim
	FulfillStorageClassClaimInitializing fulfillstorageClassClaimState = "Initializing"
	// FulfillStorageClassClaimValidating represents Validating state of FulfillStorageClassClaim
	FulfillStorageClassClaimValidating fulfillstorageClassClaimState = "Validating"
	// FulfillStorageClassClaimFailed represents Failed state of FulfillStorageClassClaim
	FulfillStorageClassClaimFailed fulfillstorageClassClaimState = "Failed"
	// FulfillStorageClassClaimCreating represents Configuring state of FulfillStorageClassClaim
	FulfillStorageClassClaimCreating fulfillstorageClassClaimState = "Creating"
	// FulfillStorageClassClaimConfiguring represents Configuring state of FulfillStorageClassClaim
	FulfillStorageClassClaimConfiguring fulfillstorageClassClaimState = "Configuring"
	// FulfillStorageClassClaimReady represents Ready state of FulfillStorageClassClaim
	FulfillStorageClassClaimReady fulfillstorageClassClaimState = "Ready"
	// FulfillStorageClassClaimDeleting represents Deleting state of FulfillStorageClassClaim
	FulfillStorageClassClaimDeleting fulfillstorageClassClaimState = "Deleting"
)

const (
	FulfillStorageClassClaimFinalizer  = "fulfillstorageclassclaim.ocs.openshift.io"
	FulfillStorageClassClaimAnnotation = "ocs.openshift.io.fulfillstoragesclassclaim"
	CephFileSystemDataPoolLabel        = "cephfilesystem.datapool.name"
)

// FulFillStorageClassClaimStatus defines the observed state of FulfillStorageClassClaim
type FulFillStorageClassClaimStatus struct {
	Phase fulfillstorageClassClaimState `json:"phase,omitempty"`
	// CephResources provide details of created ceph resources required for external storage
	CephResources []*CephResourcesSpec `json:"cephResources,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="StorageType",type="string",JSONPath=".spec.type"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"

// FulfillStorageClassClaim is the Schema for the fulfillstorageclassclaims API
type FulfillStorageClassClaim struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FulfillStorageClassClaimSpec   `json:"spec,omitempty"`
	Status FulFillStorageClassClaimStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// FulfillStorageClassClaimList contains a list of FulfillStorageClassClaim
type FulfillStorageClassClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FulfillStorageClassClaim `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FulfillStorageClassClaim{}, &FulfillStorageClassClaimList{})
}
