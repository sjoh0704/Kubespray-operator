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

package v1alpha1

import (
	coreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// type NodeInfo struct {
// 	Name      string         `json:"name,omitempty"`
// 	Ip        string         `json:"ip,omitempty"`
// 	IsMaster  bool           `json:"isMaster,omitempty"`
// 	Resources []ResourceType `json:"resources,omitempty"`
// }

type ResourceType struct {
	Type     string `json:"type,omitempty"`
	Capacity string `json:"capacity,omitempty"`
	Usage    string `json:"usage,omitempty"`
}

// ClusterManagerSpec defines the desired state of ClusterManager
type ClusterManagerSpec struct {
	// +kubebuilder:validation:Required
	// The name of cloud provider where VM is created
	Provider string `json:"provider"`
	// +kubebuilder:validation:Required
	// The version of kubernetes
	Version string `json:"version"`
	// +kubebuilder:validation:Required
	// The number of master node
	MasterNum int `json:"masterNum"`
	// +kubebuilder:validation:Required
	// The number of worker node
	WorkerNum int `json:"workerNum"`
	// The version of kubernetes
	// KubernetesVersion string `json:"kubernetesVersion"`
	// The owner of cluster
	// Owner string `json:"owner"`
}

// ProviderAwsSpec defines
type ProviderAwsSpec struct {
	// The region where VM is working
	Region string `json:"region,omitempty"`
	// The ssh key info to access VM
	Bastion Instance `json:"bastion,omitempty"`

	Master Instance `json:"master,omitempty"`

	Worker Instance `json:"worker,omitempty"`

	HostOS string `json:"hostOs,omitempty"`

	NetworkSpec NetworkSpec `json:"networkSpec,omitempty"`

	AdditionalTags map[string]string `json:"additionalTags,omitempty"`
}

type Instance struct {
	// InstanceType
	Type string `json:"type,omitempty"`
	// InstanceNum
	Num int `json:"num,omitempty"`
	// InstanceDiskSize
	DiskSize int `json:"diskSize,omitempty"`
}
type NetworkSpec struct {
	VpcCidrBlock           string   `json:"vpcCidrBlock,omitempty"`
	PrivateSubnetCidrBlock []string `json:"privateSubnetCidrBlock,omitempty"`
	PublicSubnetCidrBlock  []string `json:"publicSubnetCidrBlock,omitempty"`
}

// ProviderVsphereSpec defines
type ProviderVsphereSpec struct {
	// The internal IP address cider block for pods
	PodCidr string `json:"podCidr,omitempty"`
	// The IP address of vCenter Server Application(VCSA)
	VcenterIp string `json:"vcenterIp,omitempty"`
	// The user id of VCSA
	VcenterId string `json:"vcenterId,omitempty"`
	// The password of VCSA
	VcenterPassword string `json:"vcenterPassword,omitempty"`
	// The TLS thumbprint of machine certificate
	VcenterThumbprint string `json:"vcenterThumbprint,omitempty"`
	// The name of network
	VcenterNetwork string `json:"vcenterNetwork,omitempty"`
	// The name of data center
	VcenterDataCenter string `json:"vcenterDataCenter,omitempty"`
	// The name of data store
	VcenterDataStore string `json:"vcenterDataStore,omitempty"`
	// The name of folder
	VcenterFolder string `json:"vcenterFolder,omitempty"`
	// The name of resource pool
	VcenterResourcePool string `json:"vcenterResourcePool,omitempty"`
	// The IP address of control plane for remote cluster(vip)
	VcenterKcpIp string `json:"vcenterKcpIp,omitempty"`
	// The number of cpus for vm
	VcenterCpuNum int `json:"vcenterCpuNum,omitempty"`
	// The memory size for vm
	VcenterMemSize int `json:"vcenterMemSize,omitempty"`
	// The disk size for vm
	VcenterDiskSize int `json:"vcenterDiskSize,omitempty"`
	// The template name for cloud init
	VcenterTemplate string `json:"vcenterTemplate,omitempty"`
}

// ClusterManagerStatus defines the observed state of ClusterManager
type ClusterManagerStatus struct {
	Provider               string                  `json:"provider,omitempty"`
	Version                string                  `json:"version,omitempty"`
	Ready                  bool                    `json:"ready,omitempty"`
	ControlPlaneReady      bool                    `json:"controlPlaneReady,omitempty"`
	MasterRun              int                     `json:"masterRun,omitempty"`
	WorkerRun              int                     `json:"workerRun,omitempty"`
	NodeInfo               []coreV1.NodeSystemInfo `json:"nodeInfo,omitempty"`
	Phase                  string                  `json:"phase,omitempty"`
	ControlPlaneEndpoint   string                  `json:"controlPlaneEndpoint,omitempty"`
	ArgoReady              bool                    `json:"argoReady,omitempty"`
	TraefikReady           bool                    `json:"traefikReady,omitempty"`
	MonitoringReady        bool                    `json:"gatewayReady,omitempty"`
	AuthClientReady        bool                    `json:"authClientReady,omitempty"`
	HyperregistryOidcReady bool                    `json:"hyperregistryOidcReady,omitempty"`
	OpenSearchReady        bool                    `json:"openSearchReady,omitempty"`
	K8sReady               bool                    `json:"k8sReady,omitempty"`
	InfrastructureReady    bool                    `json:"infrastructureReady,omitempty"`
	KubeconfigReady        bool                    `json:"kubeconfigReady,omitempty"`
	// will be deprecated
	PrometheusReady bool `json:"prometheusReady,omitempty"`
}

type ClusterManagerPhase string

//type ClusterType string

const (
	// ClusterManagerPhasePending is the first state a Cluster is assigned by
	// Cluster API Cluster controller after being created.
	ClusterManagerPhasePending = ClusterManagerPhase("Pending")

	// ClusterManagerPhaseProvisioning is the state when the Cluster has a provider infrastructure
	// object associated and can start provisioning.
	ClusterManagerPhaseProvisioning = ClusterManagerPhase("Provisioning")

	// ClusterManagerPhaseProvisioned is the state when its
	// infrastructure has been created and configured.
	ClusterManagerPhaseProvisioned = ClusterManagerPhase("Provisioned")

	// ClusterManagerPhaseDeleting is the Cluster state when a delete
	// request has been sent to the API Server,
	// but its infrastructure has not yet been fully deleted.
	ClusterManagerPhaseDeleting = ClusterManagerPhase("Deleting")

	// ClusterManagerPhaseFailed is the Cluster state when the system
	// might require user intervention.
	ClusterManagerPhaseFailed = ClusterManagerPhase("Failed")

	// ClusterManagerPhaseUnknown is returned if the Cluster state cannot be determined.
	ClusterManagerPhaseUnknown = ClusterManagerPhase("Unknown")

	ClusterManagerFinalizer = "clustermanager.cluster.tmax.io/finalizer"

	// ClusterTypeCreated    = ClusterType("created")
	// ClusterTypeRegistered = ClusterType("registered")
	ClusterTypeCreated    = "created"
	ClusterTypeRegistered = "registered"

	AnnotationKeyClmApiserver = "clustermanager.cluster.tmax.io/apiserver"
	AnnotationKeyClmGateway   = "clustermanager.cluster.tmax.io/gateway"
	AnnotationKeyClmSuffix    = "clustermanager.cluster.tmax.io/suffix"
	AnnotationKeyClmDomain    = "clustermanager.cluster.tmax.io/domain"

	LabelKeyClmName               = "clustermanager.cluster.tmax.io/clm-name"
	LabelKeyClmNamespace          = "clustermanager.cluster.tmax.io/clm-namespace"
	LabelKeyClcName               = "clustermanager.cluster.tmax.io/clc-name"
	LabelKeyClrName               = "clustermanager.cluster.tmax.io/clr-name"
	LabelKeyClmClusterType        = "clustermanager.cluster.tmax.io/cluster-type"
	LabelKeyClmClusterTypeDefunct = "type"

	// LabelKeyClmClusterTypeDefunct = "type"
	// LabelKeyClcNameDefunct = "parent"
	// LabelKeyClrNameDefunct = "parent"
)

func (c *ClusterManagerStatus) SetTypedPhase(p ClusterManagerPhase) {
	c.Phase = string(p)
}

type JobType string

const (
	ProvisioningInfrastrucutre = "provisioning-infrastructure"
	InstallingK8s              = "installing-k8s"
	CreatingKubeconfig         = "creating-kubeconfig"

	AnnotationKeyJobType = "clustermanager.cluster.tmax.io/job-type"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=clustermanagers,scope=Namespaced,shortName=clm
// +kubebuilder:printcolumn:name="Provider",type="string",JSONPath=".spec.provider",description="provider"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.version",description="k8s version"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.ready",description="is running"
// +kubebuilder:printcolumn:name="MasterNum",type="string",JSONPath=".spec.masterNum",description="replica number of master"
// +kubebuilder:printcolumn:name="MasterRun",type="string",JSONPath=".status.masterRun",description="running of master"
// +kubebuilder:printcolumn:name="WorkerNum",type="string",JSONPath=".spec.workerNum",description="replica number of worker"
// +kubebuilder:printcolumn:name="WorkerRun",type="string",JSONPath=".status.workerRun",description="running of worker"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="cluster status phase"
// ClusterManager is the Schema for the clustermanagers API
type ClusterManager struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec        ClusterManagerSpec   `json:"spec"`
	Status      ClusterManagerStatus `json:"status,omitempty"`
	AwsSpec     ProviderAwsSpec      `json:"awsSpec,omitempty"`
	VsphereSpec ProviderVsphereSpec  `json:"vsphereSpec,omitempty"`
}

// +kubebuilder:object:root=true
// ClusterManagerList contains a list of ClusterManager
type ClusterManagerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterManager `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterManager{}, &ClusterManagerList{})
}

func (c *ClusterManager) GetNamespacedName() types.NamespacedName {
	return types.NamespacedName{
		Name:      c.Name,
		Namespace: c.Namespace,
	}
}
