/*
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

package controllers

import (
	"context"
	"fmt"
	"strings"

	clusterV1alpha1 "github.com/tmax-cloud/hypercloud-multi-operator/apis/cluster/v1alpha1"
	"github.com/tmax-cloud/hypercloud-multi-operator/controllers/util"
	batchV1 "k8s.io/api/batch/v1"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *ClusterManagerReconciler) requeueClusterManagersForJob(o client.Object) []ctrl.Request {
	job := o.DeepCopyObject().(*batchV1.Job)

	if job.Status.Failed == 0 && job.Status.Succeeded == 0 {
		return nil
	}

	log := r.Log.WithValues("objectMapper", "clusterToClusterManager", "namespace", job.Namespace, job.Kind, job.Name)
	log.Info("Start to requeueClusterManagersForJob mapping...")

	clm := &clusterV1alpha1.ClusterManager{}

	key := types.NamespacedName{
		Name:      job.Labels[clusterV1alpha1.LabelKeyClmName],
		Namespace: job.Labels[clusterV1alpha1.LabelKeyClmNamespace],
	}

	err := r.Get(context.TODO(), key, clm)
	if errors.IsNotFound(err) {
		log.Error(err, "Cannot find cluster manager")
		return nil
	} else if err != nil {
		log.Error(err, "Failed to get cluster manager")
		return nil
	}

	helper, _ := patch.NewHelper(clm, r.Client)
	defer func() {
		if err := helper.Patch(context.TODO(), clm); err != nil {
			log.Error(err, "ClusterManager patch error")
		}
	}()

	if job.Status.Failed == 1 {
		podList := &coreV1.PodList{}
		listOpts := []client.ListOption{
			client.InNamespace(job.Namespace),
			client.MatchingLabels(map[string]string{
				"job-name": job.Name,
			}),
		}
		if err := r.List(context.TODO(), podList, listOpts...); err != nil {
			log.Error(err, "Failed to list pods")
			return nil
		}
		if len(podList.Items) > 1 {
			log.Error(fmt.Errorf("pod cannot exist more than one"), "Failed to reconcile job")
			return nil
		}

		failedReason, err := util.GetTerminatedPodStateReason(&podList.Items[0])
		if err != nil {
			log.Error(err, "Failed to reconcile job")
			return nil
		}

		if job.Annotations[clusterV1alpha1.AnnotationKeyJobType] == clusterV1alpha1.ProvisioningInfrastrucutre {

			util.SetStatusCondition(&clm.Status.Conditions,
				clusterV1alpha1.InfrastructureProvisionedReadyCondition,
				clusterV1alpha1.InfrastructureProvisioningReconciliationFailedReason,
				metaV1.ConditionFalse)

			log.Error(fmt.Errorf(failedReason), "Failed to provision infrastructure")

		} else if job.Annotations[clusterV1alpha1.AnnotationKeyJobType] == clusterV1alpha1.InstallingK8s {

			util.SetStatusCondition(&clm.Status.Conditions,
				clusterV1alpha1.K8sInstalledReadyCondition,
				clusterV1alpha1.K8sInstallingReconciliationFailedReason,
				metaV1.ConditionFalse)

			log.Error(fmt.Errorf(failedReason), "Failed to install k8s")

		} else if job.Annotations[clusterV1alpha1.AnnotationKeyJobType] == clusterV1alpha1.CreatingKubeconfig {

			util.SetStatusCondition(&clm.Status.Conditions,
				clusterV1alpha1.KubeconfigCreatedReadyCondition,
				clusterV1alpha1.KubeconfigCreatingReconciliationFailedReason,
				metaV1.ConditionFalse)

			log.Error(fmt.Errorf(failedReason), "Failed to create kubeconfig")
		} else {
			return nil
		}

		clm.Status.FailureReason = &failedReason

		return []ctrl.Request{
			{
				NamespacedName: clm.GetNamespacedName(),
			},
		}

	} else if job.Status.Succeeded == 1 {

		if job.Annotations[clusterV1alpha1.AnnotationKeyJobType] == clusterV1alpha1.ProvisioningInfrastrucutre {

			util.SetStatusCondition(&clm.Status.Conditions,
				clusterV1alpha1.InfrastructureProvisionedReadyCondition,
				clusterV1alpha1.InfrastructureProvisioningReconciliationSucceededReason,
				metaV1.ConditionTrue)

			log.Info("Created infrastructure successfully")

		} else if job.Annotations[clusterV1alpha1.AnnotationKeyJobType] == clusterV1alpha1.InstallingK8s {

			util.SetStatusCondition(&clm.Status.Conditions,
				clusterV1alpha1.K8sInstalledReadyCondition,
				clusterV1alpha1.K8sInstallingReconciliationSucceededReason,
				metaV1.ConditionTrue)

			log.Info("Installed k8s successfully")

		} else if job.Annotations[clusterV1alpha1.AnnotationKeyJobType] == clusterV1alpha1.CreatingKubeconfig {
			util.SetStatusCondition(&clm.Status.Conditions,
				clusterV1alpha1.KubeconfigCreatedReadyCondition,
				clusterV1alpha1.KubeconfigCreatingReconciliationSucceededReason,
				metaV1.ConditionTrue)

			log.Info("Created kubeconfig successfully")

			util.SetStatusCondition(&clm.Status.Conditions,
				clusterV1alpha1.ControlplaneReadyCondition,
				clusterV1alpha1.ControlplaneReadyReason,
				metaV1.ConditionTrue)

		} else {
			return nil
		}
		clm.Status.FailureReason = nil
		return []ctrl.Request{
			{
				NamespacedName: clm.GetNamespacedName(),
			},
		}
	}
	return nil
}

// func (r *ClusterManagerReconciler) requeueClusterManagersForKubeadmControlPlane(o client.Object) []ctrl.Request {
// 	cp := o.DeepCopyObject().(*controlplanev1.KubeadmControlPlane)
// 	log := r.Log.WithValues("objectMapper", "kubeadmControlPlaneToClusterManagers", "namespace", cp.Namespace, cp.Kind, cp.Name)

// 	// Don't handle deleted kubeadmcontrolplane
// 	if !cp.ObjectMeta.DeletionTimestamp.IsZero() {
// 		log.V(4).Info("kubeadmcontrolplane has a deletion timestamp, skipping mapping.")
// 		return nil
// 	}

// 	key := types.NamespacedName{
// 		Name:      strings.Split(cp.Name, "-control-plane")[0],
// 		Namespace: cp.Namespace,
// 	}
// 	clm := &clusterV1alpha1.ClusterManager{}
// 	if err := r.Get(context.TODO(), key, clm); errors.IsNotFound(err) {
// 		log.Info("ClusterManager resource not found. Ignoring since object must be deleted")
// 		return nil
// 	} else if err != nil {
// 		log.Error(err, "Failed to get ClusterManager")
// 		return nil
// 	}

// 	//create helper for patch
// 	helper, _ := patch.NewHelper(clm, r.Client)
// 	defer func() {
// 		if err := helper.Patch(context.TODO(), clm); err != nil {
// 			log.Error(err, "ClusterManager patch error")
// 		}
// 	}()

// 	clm.Status.MasterRun = int(cp.Status.Replicas)

// 	return nil
// }

// func (r *ClusterManagerReconciler) requeueClusterManagersForMachineDeployment(o client.Object) []ctrl.Request {
// 	md := o.DeepCopyObject().(*capiV1alpha3.MachineDeployment)
// 	log := r.Log.WithValues("objectMapper", "MachineDeploymentToClusterManagers", "namespace", md.Namespace, md.Kind, md.Name)

// 	// Don't handle deleted machinedeployment
// 	if !md.ObjectMeta.DeletionTimestamp.IsZero() {
// 		log.V(4).Info("machinedeployment has a deletion timestamp, skipping mapping.")
// 		return nil
// 	}

// 	//get ClusterManager
// 	key := types.NamespacedName{
// 		Name:      strings.Split(md.Name, "-md-0")[0],
// 		Namespace: md.Namespace,
// 	}
// 	clm := &clusterV1alpha1.ClusterManager{}
// 	if err := r.Get(context.TODO(), key, clm); errors.IsNotFound(err) {
// 		log.Info("ClusterManager is deleted")
// 		return nil
// 	} else if err != nil {
// 		log.Error(err, "Failed to get ClusterManager")
// 		return nil
// 	}

// 	//create helper for patch
// 	helper, _ := patch.NewHelper(clm, r.Client)
// 	defer func() {
// 		if err := helper.Patch(context.TODO(), clm); err != nil {
// 			log.Error(err, "ClusterManager patch error")
// 		}
// 	}()

// 	clm.Status.WorkerRun = int(md.Status.Replicas)

// 	return nil
// }

func (r *ClusterManagerReconciler) requeueClusterManagersForSubresources(o client.Object) []ctrl.Request {
	log := r.Log.WithValues("objectMapper", "SubresourcesToClusterManagers", "namespace", o.GetNamespace(), "name", o.GetName())

	//get ClusterManager
	key := types.NamespacedName{
		Name:      o.GetLabels()[clusterV1alpha1.LabelKeyClmName],
		Namespace: o.GetNamespace(),
	}
	clm := &clusterV1alpha1.ClusterManager{}
	if err := r.Get(context.TODO(), key, clm); errors.IsNotFound(err) {
		log.Info("ClusterManager is deleted")
		return nil
	} else if err != nil {
		log.Error(err, "Failed to get ClusterManager")
		return nil
	}

	if !clm.GetDeletionTimestamp().IsZero() {
		return nil
	}

	isGateway := strings.Contains(o.GetName(), "gateway")
	if isGateway {
		util.SetStatusCondition(&clm.Status.Conditions,
			clusterV1alpha1.TraefikReadyCondition,
			clusterV1alpha1.TraefikNotReadyReason,
			metaV1.ConditionFalse)
	} else {
		util.SetStatusCondition(&clm.Status.Conditions,
			clusterV1alpha1.TraefikReadyCondition,
			clusterV1alpha1.TraefikNotReadyReason,
			metaV1.ConditionFalse)
	}

	err := r.Status().Update(context.TODO(), clm)
	if err != nil {
		log.Error(err, "Failed to update ClusterManager status")
		return nil //??
	}

	return nil
}
