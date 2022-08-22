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
	"time"

	"github.com/go-logr/logr"
	claimV1alpha1 "github.com/tmax-cloud/hypercloud-multi-operator/apis/claim/v1alpha1"
	clusterV1alpha1 "github.com/tmax-cloud/hypercloud-multi-operator/apis/cluster/v1alpha1"
	"github.com/tmax-cloud/hypercloud-multi-operator/controllers/util"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"

	kerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var AutoAdmit bool

const (
	requeueAfter10Second = 10 * time.Second
	requeueAfter20Second = 20 * time.Second
	requeueAfter30Second = 30 * time.Second
	requeueAfter1Minute  = 1 * time.Minute
)

// ClusterClaimReconciler reconciles a ClusterClaim object
type ClusterClaimReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=claim.tmax.io,resources=clusterclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=claim.tmax.io,resources=clusterclaims/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.tmax.io,resources=clustermanagers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.tmax.io,resources=clustermanagers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete

// cluster claim 이 생성되면, reconcile 함수는 해당 cluster claim 의 status 를 awaiting 으로 변경해준다.
// 해당 claim 으로 생성한 cluster 에 대한 cluster manager 의 생성은 hypercloud-api-server 에서 진행된다.
func (r *ClusterClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = context.Background()
	log := r.Log.WithValues("ClusterClaim", req.NamespacedName)

	// get ClusterClaim
	clusterClaim := &claimV1alpha1.ClusterClaim{}
	if err := r.Get(context.TODO(), req.NamespacedName, clusterClaim); errors.IsNotFound(err) {
		log.Info("ClusterClaim resource not found. Ignoring since object must be deleted")
		return ctrl.Result{}, nil
	} else if err != nil {
		log.Error(err, "Failed to get ClusterClaim")
		return ctrl.Result{}, err
	}

	if AutoAdmit == false {
		if clusterClaim.Status.Phase == "" {
			clusterClaim.Status.Phase = "Awaiting"
			clusterClaim.Status.Reason = "Waiting for admin approval"
			err := r.Status().Update(context.TODO(), clusterClaim)
			if err != nil {
				log.Error(err, "Failed to update ClusterClaim status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		} else if clusterClaim.Status.Phase == "Awaiting" {
			return ctrl.Result{}, nil
		}
	}

	return r.reconcile(context.TODO(), clusterClaim)
}

// reconcile handles cluster reconciliation.
func (r *ClusterClaimReconciler) reconcile(ctx context.Context, clusterClaim *claimV1alpha1.ClusterClaim) (ctrl.Result, error) {
	phases := []func(context.Context, *claimV1alpha1.ClusterClaim) (ctrl.Result, error){}
	phases = append(
		phases,
		r.CreateClusterManager,
		r.CreatePersistentVolumeClaim,
	)

	res := ctrl.Result{}
	errs := []error{}
	// phases 를 돌면서, append 한 함수들을 순차적으로 수행하고,
	// error가 있는지 체크하여 error가 있으면 무조건 requeue
	// 이때는 가장 최초로 error가 발생한 phase의 requeue after time을 따라감
	// 모든 error를 최종적으로 aggregate하여 반환할 수 있도록 리스트로 반환
	// error는 없지만 다시 requeue 가 되어야 하는 phase들이 존재하는 경우
	// LowestNonZeroResult 함수를 통해 requeueAfter time 이 가장 짧은 함수를 찾는다.
	for _, phase := range phases {
		// Call the inner reconciliation methods.
		phaseResult, err := phase(ctx, clusterClaim)
		if err != nil {
			errs = append(errs, err)
		}
		if len(errs) > 0 {
			continue
		}

		// Aggregate phases which requeued without err
		res = util.LowestNonZeroResult(res, phaseResult)
	}

	return res, kerrors.NewAggregate(errs)
}

func (r *ClusterClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	controller, err := ctrl.NewControllerManagedBy(mgr).
		For(&claimV1alpha1.ClusterClaim{}).
		WithEventFilter(
			predicate.Funcs{
				CreateFunc: func(e event.CreateEvent) bool {
					return true
				},
				UpdateFunc: func(e event.UpdateEvent) bool {
					return true
				},
				DeleteFunc: func(e event.DeleteEvent) bool {
					return false
				},
				GenericFunc: func(e event.GenericEvent) bool {
					return false
				},
			},
		).
		Build(r)

	if err != nil {
		return err
	}

	return controller.Watch(
		&source.Kind{Type: &clusterV1alpha1.ClusterManager{}},
		handler.EnqueueRequestsFromMapFunc(r.requeueClusterClaimsForClusterManager),
		predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				return false
			},
			CreateFunc: func(e event.CreateEvent) bool {
				return false
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				clm := e.Object.(*clusterV1alpha1.ClusterManager)
				val, ok := clm.Labels[clusterV1alpha1.LabelKeyClmClusterType]
				if ok && val == clusterV1alpha1.ClusterTypeCreated {
					return true
				}
				return false
			},
			GenericFunc: func(e event.GenericEvent) bool {
				return false
			},
		},
	)
}
