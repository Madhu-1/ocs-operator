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

package fulfillstorageclassclaim

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	snapapi "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	v1 "github.com/red-hat-storage/ocs-operator/api/v1"
	"github.com/red-hat-storage/ocs-operator/api/v1alpha1"
	controllers "github.com/red-hat-storage/ocs-operator/controllers/storageconsumer"
	rookCephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	storageClassEncryptionParamKey = "encryptionKMSID"
)

// StorageClassClaimReconciler reconciles a FulfillStorageClassClaim object
// nolint:revive
type FulfillStorageClassClaimReconciler struct {
	client.Client
	cache.Cache
	Scheme            *runtime.Scheme
	OperatorNamespace string

	log                          logr.Logger
	ctx                          context.Context
	storageConsumer              *v1alpha1.StorageConsumer
	storageCluster               *v1.StorageCluster
	fulfillStorageClassClaim     *v1alpha1.FulfillStorageClassClaim
	cephBlockPool                *rookCephv1.CephBlockPool
	cephFilesystemSubVolumeGroup *rookCephv1.CephFilesystemSubVolumeGroup
	cephClientProvisioner        *rookCephv1.CephClient
	cephClientNode               *rookCephv1.CephClient
	cephResourcesByName          map[string]*v1alpha1.CephResourcesSpec
	storageProfile               *v1.StorageProfile
}

// +kubebuilder:rbac:groups=ocs.openshift.io,resources=fulfillstorageclassclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ocs.openshift.io,resources=fulfillstorageclassclaims/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ceph.rook.io,resources=cephclients,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=ceph.rook.io,resources=cephfilesystemsubvolumegroups,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=ceph.rook.io,resources=cephblockpools,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=ocs.openshift.io,resources=storageclusters,verbs=get;watch;list
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotclasses,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch

func (r *FulfillStorageClassClaimReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	if ok := r.Cache.WaitForCacheSync(ctx); !ok {
		return reconcile.Result{}, fmt.Errorf("cache sync failed")
	}

	r.log = ctrllog.FromContext(ctx, "FulfillStorageClassClaim", request)
	r.ctx = ctrllog.IntoContext(ctx, r.log)
	r.log.Info("Reconciling FulfillStorageClassClaim.")

	// Fetch the FulfillStorageClassClaim instance
	r.fulfillStorageClassClaim = &v1alpha1.FulfillStorageClassClaim{}
	r.fulfillStorageClassClaim.Name = request.Name
	r.fulfillStorageClassClaim.Namespace = request.Namespace

	if err := r.get(r.fulfillStorageClassClaim); err != nil {
		if errors.IsNotFound(err) {
			r.log.Info("FulfillStorageClassClaim resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		r.log.Error(err, "Failed to get FulfillStorageClassClaim.")
		return reconcile.Result{}, err
	}

	r.fulfillStorageClassClaim.Status.Phase = v1alpha1.FulfillStorageClassClaimInitializing

	storageClusterList := &v1.StorageClusterList{}
	if err := r.list(storageClusterList, client.InNamespace(r.OperatorNamespace)); err != nil {
		return reconcile.Result{}, err
	}

	switch l := len(storageClusterList.Items); {
	case l == 0:
		return reconcile.Result{}, fmt.Errorf("no StorageCluster found")
	case l != 1:
		return reconcile.Result{}, fmt.Errorf("multiple StorageCluster found")
	}
	r.storageCluster = &storageClusterList.Items[0]

	var result reconcile.Result
	var reconcileError error
	result, reconcileError = r.reconcileProviderPhases()

	// Apply status changes to the FulfillStorageClassClaim
	statusError := r.Client.Status().Update(r.ctx, r.fulfillStorageClassClaim)
	if statusError != nil {
		r.log.Info("Failed to update FulfillStorageClassClaim status.")
	}

	// Reconcile errors have higher priority than status update errors
	if reconcileError != nil {
		return result, reconcileError
	}

	if statusError != nil {
		return result, statusError
	}

	return result, nil
}

func (r *FulfillStorageClassClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueueStorageConsumerRequest := handler.EnqueueRequestsFromMapFunc(
		func(obj client.Object) []reconcile.Request {
			annotations := obj.GetAnnotations()
			if annotation, found := annotations[v1alpha1.FulfillStorageClassClaimAnnotation]; found {
				parts := strings.Split(annotation, "/")
				return []reconcile.Request{{
					NamespacedName: types.NamespacedName{
						Namespace: parts[0],
						Name:      parts[1],
					},
				}}
			}
			return []reconcile.Request{}
		})
	// As we are not setting the Controller OwnerReference on the ceph
	// resources we are creating as part of the FulfillStorageClassClaim, we need to
	// set IsController to false to get Reconcile Request of FulfillStorageClassClaim
	// for the owned ceph resources updates.
	enqueueForNonControllerOwner := &handler.EnqueueRequestForOwner{OwnerType: &v1alpha1.FulfillStorageClassClaim{}, IsController: false}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.FulfillStorageClassClaim{}, builder.WithPredicates(
			predicate.GenerationChangedPredicate{},
		)).
		Watches(&source.Kind{Type: &rookCephv1.CephBlockPool{}}, enqueueForNonControllerOwner).
		Watches(&source.Kind{Type: &rookCephv1.CephFilesystemSubVolumeGroup{}}, enqueueForNonControllerOwner).
		Watches(&source.Kind{Type: &rookCephv1.CephClient{}}, enqueueForNonControllerOwner).
		Watches(&source.Kind{Type: &storagev1.StorageClass{}}, enqueueStorageConsumerRequest).
		Watches(&source.Kind{Type: &snapapi.VolumeSnapshotClass{}}, enqueueStorageConsumerRequest).
		Complete(r)
}

func (r *FulfillStorageClassClaimReconciler) reconcileProviderPhases() (reconcile.Result, error) {
	r.log.Info("Running FulfillStorageClassClaim controller in Converged/Provider Mode")

	r.fulfillStorageClassClaim.Status.Phase = v1alpha1.FulfillStorageClassClaimInitializing

	gvk, err := apiutil.GVKForObject(&v1alpha1.StorageConsumer{}, r.Client.Scheme())
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to get gvk for consumer  %w", err)
	}
	// reading storageConsumer Name from fulfillStorageClassClaim ownerReferences
	ownerRefs := r.fulfillStorageClassClaim.GetOwnerReferences()
	for i := range ownerRefs {
		if ownerRefs[i].Kind == gvk.Kind {
			r.storageConsumer = &v1alpha1.StorageConsumer{}
			r.storageConsumer.Name = ownerRefs[i].Name
			r.storageConsumer.Namespace = r.OperatorNamespace
			break
		}
	}
	if r.storageConsumer == nil {
		return reconcile.Result{}, fmt.Errorf("no storage consumer owner ref on the storage class claim")
	}

	if err := r.get(r.storageConsumer); err != nil {
		return reconcile.Result{}, err
	}

	// check claim status already contains the name of the resource. if not, add it.
	if r.fulfillStorageClassClaim.Spec.Type == "blockpool" {
		r.cephBlockPool = &rookCephv1.CephBlockPool{}
		r.cephBlockPool.Namespace = r.OperatorNamespace
		for _, res := range r.fulfillStorageClassClaim.Status.CephResources {
			if res.Kind == "CephBlockPool" {
				r.cephBlockPool.Name = res.Name
				break
			}
		}
		if r.cephBlockPool.Name == "" {
			r.cephBlockPool.Name = fmt.Sprintf("cephblockpool-%s-%s", r.storageConsumer.Name, generateUUID())
		}

	} else if r.fulfillStorageClassClaim.Spec.Type == "sharedfilesystem" {
		r.cephFilesystemSubVolumeGroup = &rookCephv1.CephFilesystemSubVolumeGroup{}
		r.cephFilesystemSubVolumeGroup.Namespace = r.OperatorNamespace
		for _, res := range r.fulfillStorageClassClaim.Status.CephResources {
			if res.Kind == "CephFilesystemSubVolumeGroup" {
				r.cephFilesystemSubVolumeGroup.Name = res.Name
				break
			}
		}
		if r.cephFilesystemSubVolumeGroup.Name == "" {
			r.cephFilesystemSubVolumeGroup.Name = fmt.Sprintf("cephfilesystemsubvolumegroup-%s-%s", r.storageConsumer.Name, generateUUID())
		}
	}

	profileName := r.fulfillStorageClassClaim.Spec.StorageProfile
	if profileName == "" {
		profileName = r.storageCluster.Spec.DefaultStorageProfile
	}

	for i := range r.storageCluster.Spec.StorageProfiles {
		profile := &r.storageCluster.Spec.StorageProfiles[i]
		if profile.Name == profileName {
			r.storageProfile = profile
			break
		}
	}

	if r.storageProfile == nil {
		return reconcile.Result{}, fmt.Errorf("no storage profile definition found for storage profile %s", profileName)
	}

	r.cephClientProvisioner = &rookCephv1.CephClient{}
	r.cephClientProvisioner.Name = controllers.GenerateHashForCephClient(r.fulfillStorageClassClaim.Name, "provisioner")
	r.cephClientProvisioner.Namespace = r.OperatorNamespace

	r.cephClientNode = &rookCephv1.CephClient{}
	r.cephClientNode.Name = controllers.GenerateHashForCephClient(r.fulfillStorageClassClaim.Name, "node")
	r.cephClientNode.Namespace = r.OperatorNamespace

	r.cephResourcesByName = map[string]*v1alpha1.CephResourcesSpec{}

	for _, cephResourceSpec := range r.fulfillStorageClassClaim.Status.CephResources {
		r.cephResourcesByName[cephResourceSpec.Name] = cephResourceSpec
	}

	r.fulfillStorageClassClaim.Status.Phase = v1alpha1.FulfillStorageClassClaimCreating

	if r.fulfillStorageClassClaim.GetDeletionTimestamp().IsZero() {
		if r.fulfillStorageClassClaim.Spec.Type == "blockpool" {

			if err := r.reconcileCephClientRBDProvisioner(); err != nil {
				return reconcile.Result{}, err
			}

			if err := r.reconcileCephClientRBDNode(); err != nil {
				return reconcile.Result{}, err
			}

			if err := r.reconcileCephBlockPool(); err != nil {
				return reconcile.Result{}, err
			}

		} else if r.fulfillStorageClassClaim.Spec.Type == "sharedfilesystem" {
			if err := r.reconcileCephClientCephFSProvisioner(); err != nil {
				return reconcile.Result{}, err
			}

			if err := r.reconcileCephClientCephFSNode(); err != nil {
				return reconcile.Result{}, err
			}

			if err := r.reconcileCephFilesystemSubVolumeGroup(); err != nil {
				return reconcile.Result{}, err
			}
		}
		cephResourcesReady := true
		for _, cephResource := range r.fulfillStorageClassClaim.Status.CephResources {
			if cephResource.Phase != "Ready" {
				cephResourcesReady = false
				break
			}
		}

		if cephResourcesReady {
			r.fulfillStorageClassClaim.Status.Phase = v1alpha1.FulfillStorageClassClaimReady
		}

	} else {
		r.fulfillStorageClassClaim.Status.Phase = v1alpha1.FulfillStorageClassClaimDeleting
	}
	return reconcile.Result{}, nil
}

func (r *FulfillStorageClassClaimReconciler) getCephFSStorageClass(data map[string]string) *storagev1.StorageClass {
	pvReclaimPolicy := corev1.PersistentVolumeReclaimDelete
	allowVolumeExpansion := true
	storageClass := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.fulfillStorageClassClaim.Name,
			Namespace: r.fulfillStorageClassClaim.Namespace,
			Annotations: map[string]string{
				"description": "Provides RWO and RWX Filesystem volumes",
			},
		},
		ReclaimPolicy:        &pvReclaimPolicy,
		AllowVolumeExpansion: &allowVolumeExpansion,
		Provisioner:          fmt.Sprintf("%s.cephfs.csi.ceph.com", r.storageCluster.Namespace),
		Parameters:           data,
	}
	return storageClass
}

func (r *FulfillStorageClassClaimReconciler) getCephRBDStorageClass(data map[string]string) *storagev1.StorageClass {
	pvReclaimPolicy := corev1.PersistentVolumeReclaimDelete
	allowVolumeExpansion := true
	storageClass := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.fulfillStorageClassClaim.Name,
			Namespace: r.fulfillStorageClassClaim.Namespace,
			Annotations: map[string]string{
				"description": "Provides RWO Filesystem volumes, and RWO and RWX Block volumes",
			},
		},
		ReclaimPolicy:        &pvReclaimPolicy,
		AllowVolumeExpansion: &allowVolumeExpansion,
		Provisioner:          fmt.Sprintf("%s.rbd.csi.ceph.com", r.storageCluster.Namespace),
		Parameters:           data,
	}
	return storageClass
}

func (r *FulfillStorageClassClaimReconciler) getCephFSVolumeSnapshotClass(data map[string]string) *snapapi.VolumeSnapshotClass {
	volumesnapshotclass := &snapapi.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: r.fulfillStorageClassClaim.Name,
		},
		Driver:         fmt.Sprintf("%s.cephfs.csi.ceph.com", r.storageCluster.Namespace),
		DeletionPolicy: snapapi.VolumeSnapshotContentDelete,
		Parameters:     data,
	}
	return volumesnapshotclass
}

func (r *FulfillStorageClassClaimReconciler) getCephRBDVolumeSnapshotClass(data map[string]string) *snapapi.VolumeSnapshotClass {
	volumesnapshotclass := &snapapi.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: r.fulfillStorageClassClaim.Name,
		},
		Driver:         fmt.Sprintf("%s.rbd.csi.ceph.com", r.storageCluster.Namespace),
		DeletionPolicy: snapapi.VolumeSnapshotContentDelete,
		Parameters:     data,
	}
	return volumesnapshotclass
}

func (r *FulfillStorageClassClaimReconciler) createOrReplaceStorageClass(storageClass *storagev1.StorageClass) error {
	existing := &storagev1.StorageClass{}
	existing.Name = r.fulfillStorageClassClaim.Name

	if err := r.get(existing); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to get StorageClass: %v", err)
	}

	// If present then compare the existing StorageClass with the received StorageClass, and only proceed if they differ.
	if reflect.DeepEqual(existing.Parameters, storageClass.Parameters) {
		return nil
	}

	// StorageClass already exists, but parameters have changed. Delete the existing StorageClass and create a new one.
	if existing.UID != "" {

		// Since we have to update the existing StorageClass, so we will delete the existing StorageClass and create a new one.
		r.log.Info("StorageClass needs to be updated, deleting it.", "StorageClass", klog.KRef(storageClass.Namespace, existing.Name))

		// Delete the StorageClass.
		err := r.delete(existing)
		if err != nil {
			r.log.Error(err, "Failed to delete StorageClass.", "StorageClass", klog.KRef(storageClass.Namespace, existing.Name))
			return err
		}
	}
	r.log.Info("Creating StorageClass.", "StorageClass", klog.KRef(storageClass.Namespace, existing.Name))
	err := r.Client.Create(r.ctx, storageClass)
	if err != nil {
		return fmt.Errorf("failed to create StorageClass: %v", err)
	}
	return nil
}

func (r *FulfillStorageClassClaimReconciler) createOrReplaceVolumeSnapshotClass(volumeSnapshotClass *snapapi.VolumeSnapshotClass) error {
	existing := &snapapi.VolumeSnapshotClass{}
	existing.Name = r.fulfillStorageClassClaim.Name

	if err := r.get(existing); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to get VolumeSnapshotClass: %v", err)
	}

	// If present then compare the existing VolumeSnapshotClass parameters with
	// the received VolumeSnapshotClass parameters, and only proceed if they differ.
	if reflect.DeepEqual(existing.Parameters, volumeSnapshotClass.Parameters) {
		return nil
	}

	// VolumeSnapshotClass already exists, but parameters have changed. Delete the existing VolumeSnapshotClass and create a new one.
	if existing.UID != "" {
		// Since we have to update the existing VolumeSnapshotClass, so we will delete the existing VolumeSnapshotClass and create a new one.
		r.log.Info("VolumeSnapshotClass needs to be updated, deleting it.", "Name", existing.Name)

		// Delete the VolumeSnapshotClass.
		if err := r.delete(existing); err != nil {
			r.log.Error(err, "Failed to delete VolumeSnapshotClass.", "Name", existing.Name)
			return err
		}
	}
	r.log.Info("Creating VolumeSnapshotClass.", "Name", existing.Name)
	if err := r.Client.Create(r.ctx, volumeSnapshotClass); err != nil {
		return fmt.Errorf("failed to create VolumeSnapshotClass: %v", err)
	}
	return nil
}

func (r *FulfillStorageClassClaimReconciler) reconcileCephBlockPool() error {

	failureDomain := r.storageCluster.Status.FailureDomain

	capacity := r.storageConsumer.Spec.Capacity.String()

	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.cephBlockPool, func() error {
		if err := r.own(r.cephBlockPool); err != nil {
			return err
		}
		deviceClass := r.storageProfile.DeviceClass
		deviceSetList := r.storageCluster.Spec.StorageDeviceSets
		var deviceSet *v1.StorageDeviceSet
		for i := range deviceSetList {
			ds := &deviceSetList[i]
			// get the required deviceSetName of the profile
			if deviceClass == ds.DeviceClass {
				deviceSet = ds
				break
			}
		}

		if deviceSet == nil {
			return fmt.Errorf("could not find device set definition named %s in storagecluster", deviceClass)
		}

		addLabel(r.cephBlockPool, controllers.StorageConsumerNameLabel, r.storageConsumer.Name)

		r.cephBlockPool.Spec = rookCephv1.NamedBlockPoolSpec{
			PoolSpec: rookCephv1.PoolSpec{
				FailureDomain: failureDomain,
				DeviceClass:   deviceClass,
				Replicated: rookCephv1.ReplicatedSpec{
					Size:                     3,
					ReplicasPerFailureDomain: 1,
				},
				Parameters: r.storageProfile.BlockPoolConfiguration.Parameters,
				Quotas: rookCephv1.QuotaSpec{
					MaxSize: &capacity,
				},
			},
		}
		return nil
	})

	if err != nil {
		r.log.Error(
			err,
			"Failed to update CephBlockPool.",
			"CephBlockPool",
			klog.KRef(r.cephBlockPool.Namespace, r.cephBlockPool.Name),
		)
		return err
	}

	cephClients := map[string]string{
		"provisioner": r.cephClientProvisioner.Name,
		"node":        r.cephClientNode.Name,
	}
	phase := ""
	if r.cephBlockPool.Status != nil {
		phase = string(r.cephBlockPool.Status.Phase)
	}

	r.setCephResourceStatus(r.cephBlockPool.Name, "CephBlockPool", phase, cephClients)

	return nil
}

func (r *FulfillStorageClassClaimReconciler) reconcileCephFilesystemSubVolumeGroup() error {

	cephFilesystem := rookCephv1.CephFilesystem{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-cephfilesystem", r.storageCluster.Name),
			Namespace: r.storageCluster.Namespace,
		},
	}
	if err := r.get(&cephFilesystem); err != nil {
		return fmt.Errorf("error fetching CephFilesystem. %+v", err)
	}

	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.cephFilesystemSubVolumeGroup, func() error {
		if err := r.own(r.cephFilesystemSubVolumeGroup); err != nil {
			return err
		}
		deviceClass := r.storageProfile.DeviceClass
		dataPool := &rookCephv1.NamedPoolSpec{}
		for i := range cephFilesystem.Spec.DataPools {
			if cephFilesystem.Spec.DataPools[i].DeviceClass == deviceClass {
				dataPool = &cephFilesystem.Spec.DataPools[i]
				break
			}
		}
		if dataPool == nil {
			return fmt.Errorf("no CephFileSystem found in the cluster for storage profile %s", r.fulfillStorageClassClaim.Spec.StorageProfile)
		}

		addLabel(r.cephFilesystemSubVolumeGroup, controllers.StorageConsumerNameLabel, r.storageConsumer.Name)
		// This label is required to set the dataPool on the CephFS
		// storageclass so that each PVC created from CephFS storageclass can
		// use correct dataPool backed by deviceclass.
		addLabel(r.cephFilesystemSubVolumeGroup, v1alpha1.CephFileSystemDataPoolLabel, fmt.Sprintf("%s-%s", cephFilesystem.Name, dataPool.Name))

		r.cephFilesystemSubVolumeGroup.Spec = rookCephv1.CephFilesystemSubVolumeGroupSpec{
			FilesystemName: cephFilesystem.Name,
		}
		return nil
	})

	if err != nil {
		r.log.Error(
			err,
			"Failed to update CephFilesystemSubVolumeGroup.",
			"CephFilesystemSubVolumeGroup",
			klog.KRef(r.cephFilesystemSubVolumeGroup.Namespace, r.cephFilesystemSubVolumeGroup.Name),
		)
		return err
	}

	cephClients := map[string]string{
		"provisioner": r.cephClientProvisioner.Name,
		"node":        r.cephClientNode.Name,
	}
	phase := ""
	if r.cephFilesystemSubVolumeGroup.Status != nil {
		phase = string(r.cephFilesystemSubVolumeGroup.Status.Phase)
	}

	r.setCephResourceStatus(r.cephFilesystemSubVolumeGroup.Name, "CephFilesystemSubVolumeGroup", phase, cephClients)

	return nil
}

func (r *FulfillStorageClassClaimReconciler) reconcileCephClientRBDProvisioner() error {
	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.cephClientProvisioner, func() error {
		if err := r.own(r.cephClientProvisioner); err != nil {
			return err
		}

		addStorageRelatedAnnotations(r.cephClientProvisioner, r.getNamespacedName(), "rbd", "provisioner")
		r.cephClientProvisioner.Spec = rookCephv1.ClientSpec{
			Caps: map[string]string{
				"mon": "profile rbd",
				"mgr": "allow rw",
				"osd": fmt.Sprintf("profile rbd pool=%s", r.cephBlockPool.Name),
			},
		}
		return nil
	})

	if err != nil {
		r.log.Error(err,
			"Failed to update CephClient.",
			"CephClient",
			klog.KRef(r.cephClientProvisioner.Namespace, r.cephClientProvisioner.Name),
		)
		return err
	}

	phase := ""
	if r.cephClientProvisioner.Status != nil {
		phase = string(r.cephClientProvisioner.Status.Phase)
	}

	r.setCephResourceStatus(r.cephClientProvisioner.Name, "CephClient", phase, nil)

	return nil
}

func (r *FulfillStorageClassClaimReconciler) reconcileCephClientRBDNode() error {
	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.cephClientNode, func() error {
		if err := r.own(r.cephClientNode); err != nil {
			return err
		}

		addStorageRelatedAnnotations(r.cephClientNode, r.getNamespacedName(), "rbd", "node")
		r.cephClientNode.Spec = rookCephv1.ClientSpec{
			Caps: map[string]string{
				"mon": "profile rbd",
				"mgr": "allow rw",
				"osd": fmt.Sprintf("profile rbd pool=%s", r.cephBlockPool.Name),
			},
		}

		return nil
	})

	if err != nil {
		r.log.Error(
			err,
			"Failed to update CephClient.",
			"CephClient",
			klog.KRef(r.cephClientNode.Namespace, r.cephClientNode.Name),
		)
		return err
	}

	phase := ""
	if r.cephClientNode.Status != nil {
		phase = string(r.cephClientNode.Status.Phase)
	}

	r.setCephResourceStatus(r.cephClientNode.Name, "CephClient", phase, nil)

	return nil
}

func (r *FulfillStorageClassClaimReconciler) reconcileCephClientCephFSProvisioner() error {

	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.cephClientProvisioner, func() error {
		if err := r.own(r.cephClientProvisioner); err != nil {
			return err
		}

		addStorageRelatedAnnotations(r.cephClientProvisioner, r.getNamespacedName(), "cephfs", "provisioner")
		r.cephClientProvisioner.Spec = rookCephv1.ClientSpec{
			Caps: map[string]string{
				"mon": "allow r",
				"mgr": "allow rw",
				"mds": fmt.Sprintf("allow rw path=/volumes/%s", r.cephFilesystemSubVolumeGroup.Name),
				"osd": "allow rw tag cephfs metadata=*",
			},
		}
		return nil
	})

	if err != nil {
		r.log.Error(
			err,
			"Failed to update CephClient.",
			"CephClient",
			klog.KRef(r.cephClientProvisioner.Namespace, r.cephClientProvisioner.Name),
		)
		return err
	}

	phase := ""
	if r.cephClientProvisioner.Status != nil {
		phase = string(r.cephClientProvisioner.Status.Phase)
	}

	r.setCephResourceStatus(r.cephClientProvisioner.Name, "CephClient", phase, nil)

	return nil
}

func (r *FulfillStorageClassClaimReconciler) reconcileCephClientCephFSNode() error {

	_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.cephClientNode, func() error {
		if err := r.own(r.cephClientNode); err != nil {
			return err
		}

		addStorageRelatedAnnotations(r.cephClientNode, r.getNamespacedName(), "cephfs", "node")
		r.cephClientNode.Spec = rookCephv1.ClientSpec{
			Caps: map[string]string{
				"mon": "allow r",
				"mgr": "allow rw",
				"osd": "allow rw tag cephfs *=*",
				"mds": fmt.Sprintf("allow rw path=/volumes/%s", r.cephFilesystemSubVolumeGroup.Name),
			},
		}
		return nil
	})

	if err != nil {
		r.log.Error(
			err,
			"Failed to update CephClient.",
			"CephClient",
			klog.KRef(r.cephClientNode.Namespace, r.cephClientNode.Name),
		)
		return err
	}

	phase := ""
	if r.cephClientNode.Status != nil {
		phase = string(r.cephClientNode.Status.Phase)
	}

	r.setCephResourceStatus(r.cephClientNode.Name, "CephClient", phase, nil)

	return nil
}

func (r *FulfillStorageClassClaimReconciler) setCephResourceStatus(name string, kind string, phase string, cephClients map[string]string) {

	cephResourceSpec := r.cephResourcesByName[name]

	if cephResourceSpec == nil {
		cephResourceSpec = &v1alpha1.CephResourcesSpec{
			Name:        name,
			Kind:        kind,
			CephClients: cephClients,
		}
		r.fulfillStorageClassClaim.Status.CephResources = append(r.fulfillStorageClassClaim.Status.CephResources, cephResourceSpec)
		r.cephResourcesByName[name] = cephResourceSpec
	}

	cephResourceSpec.Phase = phase
}

func addStorageRelatedAnnotations(obj client.Object, fulfillstorageClassClaimNamespacedName, storageClaim, cephUserType string) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
		obj.SetAnnotations(annotations)
	}

	annotations[v1alpha1.FulfillStorageClassClaimAnnotation] = fulfillstorageClassClaimNamespacedName
	annotations[controllers.StorageClaimAnnotation] = storageClaim
	annotations[controllers.StorageCephUserTypeAnnotation] = cephUserType
}

func (r *FulfillStorageClassClaimReconciler) get(obj client.Object) error {
	key := client.ObjectKeyFromObject(obj)
	return r.Client.Get(r.ctx, key, obj)
}

func (r *FulfillStorageClassClaimReconciler) update(obj client.Object) error {
	return r.Client.Update(r.ctx, obj)
}

func (r *FulfillStorageClassClaimReconciler) list(obj client.ObjectList, listOptions ...client.ListOption) error {
	return r.Client.List(r.ctx, obj, listOptions...)
}

func (r *FulfillStorageClassClaimReconciler) delete(obj client.Object) error {
	if err := r.Client.Delete(r.ctx, obj); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *FulfillStorageClassClaimReconciler) own(resource metav1.Object) error {
	// Ensure FulfillStorageClassClaim ownership on a resource
	return controllerutil.SetOwnerReference(r.fulfillStorageClassClaim, resource, r.Scheme)
}

func (r *FulfillStorageClassClaimReconciler) getNamespacedName() string {
	return fmt.Sprintf("%s/%s", r.fulfillStorageClassClaim.Namespace, r.fulfillStorageClassClaim.Name)
}

func contains(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// Removes a given string from a slice and returns the new slice
func remove(slice []string, s string) (result []string) {
	for _, item := range slice {
		if item == s {
			continue
		}
		result = append(result, item)
	}
	return
}

// addLabel add a label to a resource metadata
func addLabel(obj metav1.Object, key string, value string) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
		obj.SetLabels(labels)
	}
	labels[key] = value
}

// addAnnotation add a annotation to a resource metadata
func addAnnotation(obj metav1.Object, key string, value string) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
		obj.SetAnnotations(annotations)
	}
	annotations[key] = value
}

// generateUUID generates a random UUID string and return first 8 characters.
func generateUUID() string {
	newUUID := uuid.New().String()
	return newUUID[:8]
}
