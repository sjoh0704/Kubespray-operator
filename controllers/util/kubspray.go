package util

const (
	AP_NORTHEAST_2 string = "ap-northeast-2" // 서울
	EU_WEST_1      string = "eu-west-1"      // 아일랜드
)

var AwsRegionSet []string = []string{
	AP_NORTHEAST_2,
	EU_WEST_1,
}

const (
	UBUNTU string = "ubuntu"
	RHEL   string = "rhel"
)

var AwsSupportOs []string = []string{
	UBUNTU,
	RHEL,
}

type AwsRegionParams struct {
	AmiName  string
	AmiOwner string
	User     string
}

var TerraformAwsEnv map[string]string = map[string]string{
	"TF_VAR_AWS_DEFAULT_REGION":        "ap-northeast-2",
	"TF_VAR_AWS_SSH_KEY_NAME":          "aws-ec2-key",
	"TF_VAR_aws_cluster_name":          "dev",
	"TF_VAR_aws_ami_name":              "[\"ami-ubuntu-18.04-1.13.0-00-1548773800\"]",
	"TF_VAR_aws_ami_owner":             "[\"258751437250\"]",
	"TF_VAR_aws_vpc_cidr_block":        "10.0.0.0/16",
	"TF_VAR_aws_cidr_subnets_private":  "[\"10.0.6.0/24\", \"10.0.7.0/24\", \"10.0.8.0/24\"]",
	"TF_VAR_aws_cidr_subnets_public":   "[\"10.0.1.0/24\", \"10.0.2.0/24\", \"10.0.3.0/24\"]",
	"TF_VAR_aws_bastion_size":          "t3.medium",
	"TF_VAR_aws_bastion_num":           "1",
	"TF_VAR_aws_kube_master_num":       "3",
	"TF_VAR_aws_kube_master_size":      "1",
	"TF_VAR_aws_kube_master_disk_size": "t3.medium",
	"TF_VAR_aws_etcd_num":              "0",
	"TF_VAR_aws_etcd_size":             "t3.medium",
	"TF_VAR_aws_etcd_disk_size":        "50",
	"TF_VAR_aws_kube_worker_num":       "1",
	"TF_VAR_aws_kube_worker_size":      "t3.medium",
	"TF_VAR_aws_kube_worker_disk_size": "50",
	"TF_VAR_aws_src_dest_check":        "true",
	"TF_VAR_aws_elb_api_port":          "6443",
	"TF_VAR_k8s_secure_api_port":       "6443",
	"TF_VAR_default_tags":              "{}",
	"TF_VAR_inventory_file":            "/context/hosts",
	"TF_VAR_aws_elb_api_internal":      "false",
	"TF_VAR_aws_elb_api_public_subnet": "true",
	"TF_VAR_vpn_connection_enable":     "false",
	"TF_VAR_customer_gateway_ip":       "",
	"TF_VAR_local_cidr":                "",
	// TODO: secret으로 옮겨질 값
	"TF_VAR_AWS_ACCESS_KEY_ID":     "access-key",
	"TF_VAR_AWS_SECRET_ACCESS_KEY": "secret-access-key",
}
