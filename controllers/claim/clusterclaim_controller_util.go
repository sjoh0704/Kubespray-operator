package controllers

import (
	"context"
	"fmt"

	claimV1alpha1 "github.com/tmax-cloud/hypercloud-multi-operator/apis/claim/v1alpha1"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// clustermanager를 생성할 때, ssh key secret이 있는지 check
func (r *ClusterClaimReconciler) CheckSshKeySecretExist(clusterClaim *claimV1alpha1.ClusterClaim) error {
	key := types.NamespacedName{
		Name:      clusterClaim.Spec.ProviderAwsSpec.SshKeyName,
		Namespace: clusterClaim.Namespace,
	}
	sshKeySecret := &coreV1.Secret{}

	if err := r.Get(context.TODO(), key, sshKeySecret); errors.IsNotFound(err) {
		return err
	}

	findKey := "key.pem"
	if sshKeySecret.Data[findKey] == nil {
		return fmt.Errorf("[ %s ] key doesn't exist", findKey)
	}
	return nil
}
