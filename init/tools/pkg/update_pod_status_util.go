package pkg

import (
	goctx "context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/sets"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	asdbv1beta1 "github.com/aerospike/aerospike-kubernetes-operator/api/v1beta1"
	"github.com/aerospike/aerospike-kubernetes-operator/pkg/jsonpatch"
	"github.com/aerospike/aerospike-kubernetes-operator/pkg/utils"
)

const (
	FileSystemMountPoint = "/workdir/filesystem-volumes"
	BlockMountPoint      = "/workdir/block-volumes"
	BaseWipeVersion      = 6
)

var AddressTypeName = map[string]string{
	"access":               "accessEndpoints",
	"alternate-access":     "alternateAccessEndpoints",
	"tls-access":           "tlsAccessEndpoints",
	"tls-alternate-access": "tlsAlternateAccessEndpoints",
}

type Volume struct {
	podName             string
	volumeMode          string
	volumeName          string
	effectiveWipeMethod string
	effectiveInitMethod string
	attachmentType      string
	volumePath          string
}

func (v *Volume) getMountPoint() string {
	if v.volumeMode == "Block" {
		point := filepath.Join(BlockMountPoint, v.volumeName)
		return point
	}
	point := filepath.Join(FileSystemMountPoint, v.volumeName)
	return point
}

func (v *Volume) getAttachmentPath() string {
	return v.volumePath
}

func getImageVersion(image string) (version int, err error) {
	ver, err := asdbv1beta1.GetImageVersion(image)
	if err != nil {
		return 0, err
	}
	res := strings.Split(ver, ".")
	version, err = strconv.Atoi(res[0])
	if err != nil {
		return 0, err
	}
	return version, err
}

func execute(cmd []string, stderr *os.File) error {
	if len(cmd) > 0 {
		var command *exec.Cmd
		if len(cmd) > 1 {
			command = exec.Command(cmd[0], cmd[1:]...)
		} else {
			command = exec.Command(cmd[0])
		}
		if stderr != nil {
			command.Stdout = stderr
			command.Stderr = stderr
		} else {
			command.Stdout = os.Stdout
			command.Stderr = os.Stderr
		}

		if err := command.Run(); err != nil {
			return err
		}
	}
	return nil
}

func getNamespacedName(name, namespace string) types.NamespacedName {
	return types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
}

func getCluster(k8sClient client.Client, ctx goctx.Context, clusterNamespacedName types.NamespacedName) (*asdbv1beta1.AerospikeCluster, error) {
	aeroCluster := &asdbv1beta1.AerospikeCluster{}

	err := k8sClient.Get(ctx, clusterNamespacedName, aeroCluster)
	if err != nil {
		return nil, err
	}

	return aeroCluster, nil
}

func getPodImage(k8sClient client.Client, ctx goctx.Context, podNamespacedName types.NamespacedName) (string, error) {
	pod := &corev1.Pod{}

	err := k8sClient.Get(ctx, podNamespacedName, pod)
	if err != nil {
		return "", err
	}

	return pod.Spec.Containers[0].Image, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getNodeMetadata() asdbv1beta1.AerospikePodStatus {
	podPort := os.Getenv("POD_PORT")
	servicePort := os.Getenv("MAPPED_PORT")

	if tlsEnabled, _ := strconv.ParseBool(getEnv("MY_POD_TLS_ENABLED", "")); tlsEnabled {
		podPort = os.Getenv("POD_TLSPORT")
		servicePort = os.Getenv("MAPPED_TLSPORT")
	}
	podPortInt, _ := strconv.Atoi(podPort)
	servicePortInt, _ := strconv.Atoi(servicePort)
	matadata := asdbv1beta1.AerospikePodStatus{
		PodIP:          getEnv("PODIP", ""),
		HostInternalIP: getEnv("INTERNALIP", ""),
		HostExternalIP: getEnv("EXTERNALIP", ""),
		PodPort:        podPortInt,
		ServicePort:    int32(servicePortInt),
		Aerospike: asdbv1beta1.AerospikeInstanceSummary{
			ClusterName: getEnv("MY_POD_CLUSTER_NAME", ""),
			NodeID:      getEnv("NODE_ID", ""),
			TLSName:     getEnv("MY_POD_TLS_NAME", ""),
		},
	}
	return matadata
}

func getInitializedVolumes(podName *string, aeroCluster *asdbv1beta1.AerospikeCluster) []string {
	return aeroCluster.Status.Pods[*podName].InitializedVolumes
}

func getDirtyVolumes(podName *string, aeroCluster *asdbv1beta1.AerospikeCluster) []string {
	return aeroCluster.Status.Pods[*podName].DirtyVolumes
}

func getAttachedVolumes(rack *asdbv1beta1.Rack, aeroCluster *asdbv1beta1.AerospikeCluster) []asdbv1beta1.VolumeSpec {
	if volumes := rack.Storage.Volumes; len(volumes) != 0 {
		return volumes
	}
	return aeroCluster.Spec.Storage.Volumes
}

func getPersistentVolumes(volumes []asdbv1beta1.VolumeSpec) []asdbv1beta1.VolumeSpec {
	var volumeList []asdbv1beta1.VolumeSpec
	for _, volume := range volumes {
		if volume.Source.PersistentVolume != nil {
			volumeList = append(volumeList, volume)
		}
	}
	return volumeList
}

func newVolume(podName *string, vol asdbv1beta1.VolumeSpec) Volume {
	var volume Volume
	volume.podName = *podName
	volume.volumeMode = string(vol.Source.PersistentVolume.VolumeMode)
	volume.volumeName = vol.Name

	volume.effectiveWipeMethod = string(vol.WipeMethod)
	volume.effectiveInitMethod = string(vol.InitMethod)

	if vol.Aerospike != nil {
		volume.attachmentType = "aerospike"
		volume.volumePath = vol.Aerospike.Path
	} else {
		volume.attachmentType = ""
		volume.volumePath = ""
	}
	return volume
}

func initVolumes(podName *string, aeroCluster *asdbv1beta1.AerospikeCluster, volumes []string) ([]string, error) {
	var volumeNames []string
	var wg sync.WaitGroup

	rack, err := getRack(podName, aeroCluster)
	if err != nil {
		return volumeNames, fmt.Errorf("failed to get rack of pod %s %v", *podName, err)
	}
	workerThreads := rack.Storage.CleanupThreads
	persistentVolumes := getPersistentVolumes(getAttachedVolumes(rack, aeroCluster))
	guard := make(chan struct{}, workerThreads)
	for _, vol := range persistentVolumes {
		if utils.ContainsString(volumes, vol.Name) {
			continue
		}
		volume := newVolume(podName, vol)
		if volume.volumeMode == "Block" {
			if _, err := os.Stat(volume.getMountPoint()); err != nil {
				return volumeNames, fmt.Errorf("mounting point %s does not exist %v", volume.getMountPoint(), err)
			}
			if volume.effectiveInitMethod == "dd" {
				stderr, err := os.Create("/tmp/init-stderr")
				if err != nil {
					return volumeNames, err
				}
				dd := []string{"dd", "if=/dev/zero", "of=" + volume.getMountPoint(), "bs=1M"}
				wg.Add(1)
				guard <- struct{}{}
				go func(cmd []string) {
					defer wg.Done()
					if err := execute(cmd, stderr); err != nil {
						dat, err := os.ReadFile("/tmp/init-stderr")
						if err != nil {
							panic(err.Error())
						}
						println("stderr file = %s", string(dat))
						if !strings.Contains(string(dat), "No space left on device") {
							panic(err.Error())
						}
					}
					<-guard
				}(dd)
			} else if volume.effectiveInitMethod == "blkdiscard" {
				blkdiscard := []string{"blkdiscard", volume.getMountPoint()}
				wg.Add(1)
				guard <- struct{}{}
				go func(cmd []string) {
					defer wg.Done()
					if err := execute(cmd, nil); err != nil {
						panic(err.Error())
					}
					<-guard
				}(blkdiscard)
			} else if volume.effectiveInitMethod == "none" {
				println("Pass through")
			} else {
				return volumeNames, fmt.Errorf("invalid effective_init_method %s", volume.effectiveInitMethod)
			}
		} else if volume.volumeMode == "Filesystem" {
			if _, err := os.Stat(volume.getMountPoint()); err != nil {
				return volumeNames, fmt.Errorf("mounting point %s does not exist %v", volume.getMountPoint(), err)
			}
			if volume.effectiveInitMethod == "deleteFiles" {
				find := []string{"find", volume.getMountPoint(), "-type", "f", "-delete"}
				execute(find, nil)
			} else if volume.effectiveInitMethod == "none" {
				println("Pass through")
			} else {
				return volumeNames, fmt.Errorf("invalid effective_init_method %s", volume.effectiveInitMethod)
			}
		} else {
			return volumeNames, fmt.Errorf("invalid volume-mode %s", volume.volumeMode)
		}
		volumeNames = append(volumeNames, volume.volumeName)
	}
	close(guard)
	wg.Wait()
	volumeNames = append(volumeNames, volumes...)

	return volumeNames, nil
}

func getRack(podName *string, aeroCluster *asdbv1beta1.AerospikeCluster) (*asdbv1beta1.Rack, error) {
	res := strings.Split(*podName, "-")
	rackID, err := strconv.Atoi(res[len(res)-2])
	if err != nil {
		return nil, err
	}
	racks := aeroCluster.Spec.RackConfig.Racks
	for _, rack := range racks {
		if rack.ID == rackID {
			return &rack, nil
		}
	}
	return nil, fmt.Errorf("rack with rack-id %d not found", rackID)
}

func getNamespaceVolumePaths(podName *string, aeroCluster *asdbv1beta1.AerospikeCluster) (devicePaths, filePaths []string, err error) {
	rack, err := getRack(podName, aeroCluster)
	if err != nil {
		return devicePaths, filePaths, fmt.Errorf("failed to get rack of pod %s %v", *podName, err)
	}
	namespaces := rack.AerospikeConfig.Value["namespaces"].([]interface{})
	for _, namespace := range namespaces {
		storageEngine := namespace.(map[string]interface{})["storage-engine"].(map[string]interface{})
		devicePaths := sets.String{}
		filePaths := sets.String{}
		if storageEngine["devices"] != nil {
			for _, deviceInterface := range storageEngine["devices"].([]interface{}) {
				devicePaths.Insert(strings.Fields(deviceInterface.(string))...)
			}
		}
		if storageEngine["files"] != nil {
			for _, fileInterface := range storageEngine["files"].([]interface{}) {
				filePaths.Insert(strings.Fields(fileInterface.(string))...)
			}
		}
	}
	return devicePaths, filePaths, nil
}

func remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func cleanDirtyVolumes(podName *string, aeroCluster *asdbv1beta1.AerospikeCluster, dirtyVolumes []string) ([]string, error) {
	var wg sync.WaitGroup

	nsDevicePaths, _, err := getNamespaceVolumePaths(podName, aeroCluster)
	if err != nil {
		return dirtyVolumes, fmt.Errorf("failed to get namespaced volume paths %v", err)
	}
	rack, err := getRack(podName, aeroCluster)
	if err != nil {
		return dirtyVolumes, fmt.Errorf("failed to get rack of pod %s %v", *podName, err)
	}
	workerThreads := rack.Storage.CleanupThreads
	persistentVolumes := getPersistentVolumes(getAttachedVolumes(rack, aeroCluster))
	guard := make(chan struct{}, workerThreads)
	for _, vol := range persistentVolumes {
		if vol.Aerospike == nil || !utils.ContainsString(dirtyVolumes, vol.Name) || !utils.ContainsString(nsDevicePaths, vol.Aerospike.Path) {
			continue
		}
		volume := newVolume(podName, vol)
		if volume.volumeMode == "Block" {
			if _, err := os.Stat(volume.getMountPoint()); err != nil {
				return dirtyVolumes, fmt.Errorf("mounting point %s does not exist %v", volume.getMountPoint(), err)
			}
			if volume.effectiveWipeMethod == "dd" {
				stderr, err := os.Create("/tmp/init-stderr")
				if err != nil {
					return dirtyVolumes, err
				}
				dd := []string{"dd", "if=/dev/zero", "of=" + volume.getMountPoint(), "bs=1M"}
				wg.Add(1)
				guard <- struct{}{}
				go func(cmd []string) {
					defer wg.Done()
					if err := execute(cmd, stderr); err != nil {
						dat, err := os.ReadFile("/tmp/init-stderr")
						if err != nil {
							panic(err.Error())
						}
						if !strings.Contains(string(dat), "No space left on device") {
							panic(err.Error())
						}
					}
					<-guard

					dirtyVolumes = remove(dirtyVolumes, volume.volumeName)

				}(dd)
			} else if volume.effectiveWipeMethod == "blkdiscard" {
				blkdiscard := []string{"blkdiscard", volume.getMountPoint()}
				wg.Add(1)
				guard <- struct{}{}
				go func(cmd []string) {
					defer wg.Done()
					if err := execute(cmd, nil); err != nil {
						panic(err.Error())
					}
					<-guard
					dirtyVolumes = remove(dirtyVolumes, volume.volumeName)
				}(blkdiscard)
			} else {
				return dirtyVolumes, fmt.Errorf("invalid effective_init_method %s", volume.effectiveInitMethod)
			}
		}
	}
	close(guard)
	wg.Wait()
	return dirtyVolumes, nil
}

func wipeVolumes(podName *string, aeroCluster *asdbv1beta1.AerospikeCluster, dirtyVolumes []string) ([]string, error) {
	var wg sync.WaitGroup

	nsDevicePaths, nsFilePaths, err := getNamespaceVolumePaths(podName, aeroCluster)
	if err != nil {
		return dirtyVolumes, fmt.Errorf("failed to get namespaced volume paths %v", err)
	}
	rack, err := getRack(podName, aeroCluster)
	if err != nil {
		return dirtyVolumes, fmt.Errorf("failed to get rack of pod %s %v", *podName, err)
	}
	workerThreads := rack.Storage.CleanupThreads
	persistentVolumes := getPersistentVolumes(getAttachedVolumes(rack, aeroCluster))
	guard := make(chan struct{}, workerThreads)
	for _, vol := range persistentVolumes {
		if vol.Aerospike == nil {
			continue
		}
		volume := newVolume(podName, vol)
		if volume.volumeMode == "Block" {
			if utils.ContainsString(nsDevicePaths, volume.volumePath) {
				if _, err := os.Stat(volume.getMountPoint()); err != nil {
					return dirtyVolumes, fmt.Errorf("mounting point %s does not exist %v", volume.getMountPoint(), err)
				}
				if volume.effectiveWipeMethod == "dd" {
					stderr, err := os.Create("/tmp/init-stderr")
					if err != nil {
						return dirtyVolumes, err
					}
					dd := []string{"dd", "if=/dev/zero", "of=" + volume.getMountPoint(), "bs=1M", "2>", "/tmp/init-stderr"}
					wg.Add(1)
					guard <- struct{}{}
					go func(cmd []string) {
						wg.Done()
						if err := execute(cmd, stderr); err != nil {
							dat, err := os.ReadFile("/tmp/init-stderr")
							if err != nil {
								panic(err.Error())
							}
							if !strings.Contains(string(dat), "No space left on device") {
								panic(err.Error())
							}
						}
						<-guard

						dirtyVolumes = remove(dirtyVolumes, volume.volumeName)

					}(dd)
				} else if volume.effectiveWipeMethod == "blkdiscard" {
					blkdiscard := []string{"blkdiscard", volume.getMountPoint()}
					wg.Add(1)
					guard <- struct{}{}
					go func(cmd []string) {
						wg.Done()
						if err := execute(cmd, nil); err != nil {
							panic(err.Error())
						}
						<-guard
						dirtyVolumes = remove(dirtyVolumes, volume.volumeName)
					}(blkdiscard)
				} else {
					return dirtyVolumes, fmt.Errorf("invalid effective_init_method %s", volume.effectiveInitMethod)
				}
			}
		} else if volume.volumeMode == "Filesystem" {
			if volume.effectiveWipeMethod == "deleteFiles" {
				if _, err := os.Stat(volume.getMountPoint()); err != nil {
					return dirtyVolumes, fmt.Errorf("mounting point %s does not exist %v", volume.getMountPoint(), err)
				}
				for _, nsFilePath := range nsFilePaths {
					if strings.HasPrefix(nsFilePath, volume.getAttachmentPath()) {
						_, fileName := filepath.Split(nsFilePath)
						filePath := filepath.Join(volume.getMountPoint(), fileName)
						if _, err := os.Stat(filePath); err == nil {
							os.Remove(filePath)
						} else if errors.IsNotFound(err) {
							println("file not exist")
						} else {
							return dirtyVolumes, fmt.Errorf("failed to delete file %s %v", filePath, err)
						}
					}
				}
			} else {
				return dirtyVolumes, fmt.Errorf("invalid effective_wipe_method %s", volume.effectiveWipeMethod)
			}
		} else {
			return dirtyVolumes, fmt.Errorf("invalid volume-mode %s", volume.volumeMode)
		}
	}
	close(guard)
	wg.Wait()
	return dirtyVolumes, nil
}

func ManageVolumesAndUpdateStatus(podName, namespace, clusterName, restartType *string) error {
	cfg := ctrl.GetConfigOrDie()

	err := clientgoscheme.AddToScheme(clientgoscheme.Scheme)
	if err != nil {
		return err
	}

	err = asdbv1beta1.AddToScheme(clientgoscheme.Scheme)
	if err != nil {
		return err
	}
	k8sClient, err := client.New(
		cfg, client.Options{Scheme: clientgoscheme.Scheme},
	)

	clusterNamespacedName := getNamespacedName(*clusterName, *namespace)
	aeroCluster, err := getCluster(k8sClient, goctx.TODO(), clusterNamespacedName)
	if err != nil {
		return err
	}

	podNamespacedName := getNamespacedName(*podName, *namespace)

	podImage, err := getPodImage(k8sClient, goctx.TODO(), podNamespacedName)
	if err != nil {
		return err
	}

	prevImage := ""

	if _, ok := aeroCluster.Status.Pods[*podName]; ok {
		prevImage = aeroCluster.Status.Pods[*podName].Image
	}
	nextMajorVer, err := getImageVersion(podImage)
	if err != nil {
		return err
	}
	metadata := getNodeMetadata()

	volumes := getInitializedVolumes(podName, aeroCluster)
	dirtyVolumes := getDirtyVolumes(podName, aeroCluster)

	if *restartType == "podRestart" {
		volumes, err = initVolumes(podName, aeroCluster, volumes)
		if err != nil {
			return err
		}
		if prevImage != "" {
			prevMajorVer, err := getImageVersion(prevImage)
			if err != nil {
				return err
			}
			if (nextMajorVer >= BaseWipeVersion && BaseWipeVersion < prevMajorVer) || (nextMajorVer < BaseWipeVersion && BaseWipeVersion <= prevMajorVer) {
				dirtyVolumes, err = wipeVolumes(podName, aeroCluster, dirtyVolumes)
				if err != nil {
					return err
				}
			}
		} else {
			println("Volumes should not be wiped")
		}
		dirtyVolumes, err = cleanDirtyVolumes(podName, aeroCluster, dirtyVolumes)
		if err != nil {
			return err
		}
	}
	if err := updateStatus(k8sClient, goctx.TODO(), aeroCluster, podName, podImage, metadata, volumes, dirtyVolumes); err != nil {
		return err
	}

	return nil
}

func updateStatus(k8sClient client.Client, ctx goctx.Context, aeroCluster *asdbv1beta1.AerospikeCluster, podName *string, podImage string, metadata asdbv1beta1.AerospikePodStatus, volumes []string, dirtyVolumes []string) error {
	data, err := os.ReadFile("aerospikeConfHash")
	if err != nil {
		return fmt.Errorf("failed to read aerospikeConfHash file %v", err)
	}
	confHash := string(data)

	data, err = os.ReadFile("networkPolicyHash")
	if err != nil {
		return fmt.Errorf("failed to read networkPolicyHash file %v", err)
	}
	networkPolicyHash := string(data)

	data, err = os.ReadFile("podSpecHash")
	if err != nil {
		return fmt.Errorf("failed to read podSpecHash file %v", err)
	}

	podSpecHash := string(data)
	metadata.Image = podImage
	metadata.InitializedVolumes = volumes
	metadata.DirtyVolumes = dirtyVolumes
	metadata.AerospikeConfigHash = confHash
	metadata.NetworkPolicyHash = networkPolicyHash
	metadata.PodSpecHash = podSpecHash

	for podAddrName, confAddrName := range AddressTypeName {
		switch confAddrName {
		case "access":
			metadata.Aerospike.AccessEndpoints = getEndpoints(podAddrName)
		case "alternate-access":
			metadata.Aerospike.AlternateAccessEndpoints = getEndpoints(podAddrName)
		case "tls-access":
			metadata.Aerospike.TLSAccessEndpoints = getEndpoints(podAddrName)
		case "tls-alternate-access":
			metadata.Aerospike.TLSAlternateAccessEndpoints = getEndpoints(podAddrName)
		}
	}

	var patches []jsonpatch.JsonPatchOperation
	patch := jsonpatch.JsonPatchOperation{
		Operation: "replace",
		Path:      "/status/pods/" + *podName,
		Value:     metadata,
	}
	patches = append(patches, patch)
	jsonPatchJSON, err := json.Marshal(patches)
	if err != nil {
		return fmt.Errorf("error creating json-patch : %v", err)
	}
	constantPatch := client.RawPatch(types.JSONPatchType, jsonPatchJSON)
	if err = k8sClient.Status().Patch(
		ctx, aeroCluster, constantPatch, client.FieldOwner("pod"),
	); err != nil {
		return fmt.Errorf("error updating status: %v", err)
	}

	return nil
}

func getEndpoints(addressType string) []string {
	var endpoint []string
	addrType := strings.ReplaceAll(addressType, "-", "_")
	globalAddr := "global_" + addrType + "_address"
	globalPort := "global_" + addrType + "_port"
	host := net.ParseIP(os.Getenv(globalAddr))

	port := os.Getenv(globalPort)
	if host.To4() != nil {
		accessPoint := string(host) + ":" + port
		endpoint = append(endpoint, accessPoint)
	} else if host.To16() != nil {
		accessPoint := "[" + string(host) + "]" + ":" + port
		endpoint = append(endpoint, accessPoint)
	} else {
		panic("invalid address-type")
	}

	return endpoint
}
