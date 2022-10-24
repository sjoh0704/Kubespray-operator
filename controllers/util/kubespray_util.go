package util

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	clusterV1alpha1 "github.com/tmax-cloud/hypercloud-multi-operator/apis/cluster/v1alpha1"
	coreV1 "k8s.io/api/core/v1"
)

func CreateEnvFromClustermanagerSpec(clusterManager *clusterV1alpha1.ClusterManager) ([]coreV1.EnvVar, error) {

	AwsSpec := clusterManager.AwsSpec
	terraformAwsEnv := make(map[string]string)
	godotenv.Load(".env")

	// map deep copy
	for k, v := range TerraformAwsEnv {
		terraformAwsEnv[k] = v
	}

	// TODO crendential한 값은 secret으로 대체
	// terraform aws credential

	awsAcessKey := os.Getenv("TF_VAR_AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("TF_VAR_AWS_SECRET_ACCESS_KEY")
	if awsAcessKey == "" || awsSecretAccessKey == "" {
		return nil, fmt.Errorf("AWS ACCESS KEY or AWS SECRET ACCESS KEY doensn't exist")
	}
	terraformAwsEnv["TF_VAR_AWS_ACCESS_KEY_ID"] = awsAcessKey
	terraformAwsEnv["TF_VAR_AWS_SECRET_ACCESS_KEY"] = awsSecretAccessKey

	// aws region, os, sshkey
	terraformAwsEnv["TF_VAR_AWS_DEFAULT_REGION"] = AwsSpec.Region
	terraformAwsEnv["HOST_OS"] = AwsSpec.HostOS
	terraformAwsEnv["TF_VAR_AWS_SSH_KEY_NAME"] = AwsSpec.SshKeyName

	// 종속 파라미터는 region과 OS에 따라서 달라지므로 다음 메서드를 통해서
	params, err := GetAwsRegionalPreset(AwsSpec.Region, AwsSpec.HostOS)
	if err != nil {
		return nil, err
	}
	terraformAwsEnv["TF_VAR_aws_ami_name"] = params.AmiName
	terraformAwsEnv["TF_VAR_aws_ami_owner"] = params.AmiOwner
	terraformAwsEnv["USER"] = params.User

	// cluster name, master num, worker num
	terraformAwsEnv["TF_VAR_aws_cluster_name"] = clusterManager.Name
	terraformAwsEnv["TF_VAR_aws_kube_master_num"] = strconv.Itoa(clusterManager.Spec.MasterNum)
	terraformAwsEnv["TF_VAR_aws_kube_worker_num"] = strconv.Itoa(clusterManager.Spec.WorkerNum)

	// bastion
	if AwsSpec.Bastion.Num > 0 {
		terraformAwsEnv["TF_VAR_aws_bastion_num"] = strconv.Itoa(AwsSpec.Bastion.Num)
	}

	if AwsSpec.Bastion.Type != "" {
		terraformAwsEnv["TF_VAR_aws_bastion_size"] = AwsSpec.Bastion.Type
	}

	// master
	if AwsSpec.Master.Type != "" {
		terraformAwsEnv["TF_VAR_aws_kube_master_size"] = AwsSpec.Master.Type
	}

	if AwsSpec.Master.DiskSize != 0 {
		terraformAwsEnv["TF_VAR_aws_kube_master_disk_size"] = strconv.Itoa(AwsSpec.Master.DiskSize)
	}

	// worker
	if AwsSpec.Worker.Type != "" {
		terraformAwsEnv["TF_VAR_aws_kube_worker_size"] = AwsSpec.Worker.Type
	}

	if AwsSpec.Worker.DiskSize != 0 {
		terraformAwsEnv["TF_VAR_aws_kube_worker_disk_size"] = strconv.Itoa(AwsSpec.Worker.DiskSize)
	}

	// vpc
	if AwsSpec.NetworkSpec.VpcCidrBlock != "" {
		if len(AwsSpec.NetworkSpec.PrivateSubnetCidrBlock) != len(AwsSpec.NetworkSpec.PublicSubnetCidrBlock) {
			return nil, fmt.Errorf("PrivateSubnetCidrBlock and PublicSubnetCidrBlock must have same length of list")
		}

		publicCidr := ""
		for _, cidr := range AwsSpec.NetworkSpec.PublicSubnetCidrBlock {
			publicCidr += fmt.Sprintf("\"%s\", ", cidr)
		}

		privateCidr := ""
		for _, cidr := range AwsSpec.NetworkSpec.PrivateSubnetCidrBlock {
			privateCidr += fmt.Sprintf("\"%s\", ", cidr)
		}

		terraformAwsEnv["TF_VAR_aws_vpc_cidr_block"] = AwsSpec.NetworkSpec.VpcCidrBlock
		terraformAwsEnv["TF_VAR_aws_cidr_subnets_public"] = fmt.Sprintf("[%s]", publicCidr[:len(publicCidr)-2])
		terraformAwsEnv["TF_VAR_aws_cidr_subnets_private"] = fmt.Sprintf("[%s]", privateCidr[:len(privateCidr)-2])
	}

	// additional tags
	if len(AwsSpec.AdditionalTags) > 0 {
		addtionalTags := ""
		for key, value := range AwsSpec.AdditionalTags {
			addtionalTags += fmt.Sprintf("%s=\"%s\", ", key, value)
		}
		terraformAwsEnv["TF_VAR_default_tags"] = fmt.Sprintf("{ %s }", addtionalTags[:len(addtionalTags)-2])
	}

	EnvList := []coreV1.EnvVar{}
	for key, value := range terraformAwsEnv {
		EnvList = append(EnvList, coreV1.EnvVar{
			Name:  key,
			Value: value,
		})
	}

	return EnvList, nil
}

func GetTerminatedPodStateReason(pod *coreV1.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("pod doesn't exist")
	}
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == clusterV1alpha1.Kubespray {
			if containerStatus.State.Terminated != nil {
				return containerStatus.State.Terminated.Message, nil
			}
			return "", fmt.Errorf("not yet terminated")
		}
	}
	return "", fmt.Errorf("not found kubespray container")
}

func GetAwsRegionalPreset(region string, os string) (*AwsRegionParams, error) {
	if !IsAwsRegion(region) {
		return nil, fmt.Errorf("%s is not existing region", region)
	}
	if !IsAwsSupportOs(os) {
		return nil, fmt.Errorf("%s is not supporting os", region)
	}
	params := &AwsRegionParams{}
	if region == AP_NORTHEAST_2 {
		// params.SshKeyName = "ap-northeast-2-mo"
		if os == UBUNTU {
			params.AmiName = "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-20220609"
			params.AmiOwner = "099720109477"
			params.User = "ubuntu"

		} else if os == RHEL {
			params.AmiName = "RHEL-8.2.0_HVM-20210907-x86_64-0-Hourly2-GP2"
			params.AmiOwner = "309956199498"
			params.User = "ec2-user"
		}

	} else if region == EU_WEST_1 {
		// params.SshKeyName = "eu-west-1-mo"
		if os == UBUNTU {
			params.AmiName = "ami-ubuntu-18.04-1.13.0-00-1548773800"
			params.AmiOwner = "258751437250"
			params.User = "ubuntu"
		} else if os == RHEL {
			params.AmiName = "RHEL-8.2.0_HVM-20210907-x86_64-0-Hourly2-GP2"
			params.AmiOwner = "309956199498"
			params.User = "ec2-user"
		}
	}
	return params, nil
}

func IsAwsRegion(s string) bool {
	for _, r := range AwsRegionSet {
		if r == s {
			return true
		}
	}
	return false
}

func IsAwsSupportOs(s string) bool {
	for _, r := range AwsSupportOs {
		if r == s {
			return true
		}
	}
	return false
}
