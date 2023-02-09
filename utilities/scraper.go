package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	asdbv1beta1 "github.com/aerospike/aerospike-kubernetes-operator/api/v1beta1"
	"io"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"os"
	"path/filepath"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	//	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"strings"
	"time"
)

var (
	rootOutputDir   = "./scraperlogs"
	currentTime     = time.Now()
	outputDirectory = currentTime.Format("09-07-2017")

	logsDirectoryPod      = filepath.Join(rootOutputDir, outputDirectory, "Pod", "logs")
	eventlogsDirectory    = filepath.Join(rootOutputDir, outputDirectory, "Events")
	describeDirectorySTS  = filepath.Join(rootOutputDir, outputDirectory, "STS")
	describeDirectoryAero = filepath.Join(rootOutputDir, outputDirectory, "AeroCluster")
	describeDirectoryPVC  = filepath.Join(rootOutputDir, outputDirectory, "PVC")
)

func main() {
	namespaces := flag.String("namespaces", "", "comma separated namespaces from which logs needs to be collected")
	flag.Parse()

	cfg := ctrl.GetConfigOrDie()
	err := clientgoscheme.AddToScheme(clientgoscheme.Scheme)
	if err != nil {
		panic(err.Error())
	}

	err = asdbv1beta1.AddToScheme(clientgoscheme.Scheme)
	if err != nil {
		panic(err.Error())
	}
	k8sClient, err := client.New(
		cfg, client.Options{Scheme: clientgoscheme.Scheme},
	)

	// create the clientset
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(err.Error())
	}
	err = createDirStructure()
	if err != nil {
		panic(err.Error())
	}
	var nsList []string
	if *namespaces != "" {
		nsList = strings.Split(*namespaces, ",")
	}

	if len(nsList) == 0 {
		namespaceObjs, err := clientset.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			panic(err.Error())
		}
		for _, ns := range namespaceObjs.Items {
			nsList = append(nsList, ns.Name)
		}
	}

	for _, ns := range nsList {
		err := capturePodLogs(ns, clientset)
		if err != nil {
			panic(err.Error())
		}

		err = captureSTSLogs(ns, clientset)
		if err != nil {
			panic(err.Error())
		}

		err = captureAeroclusterLogs(ns, k8sClient)
		if err != nil {
			panic(err.Error())
		}

		err = capturePVCLogs(ns, clientset)
		if err != nil {
			panic(err.Error())
		}

		err = captureEvents(ns, clientset)
		if err != nil {
			panic(err.Error())
		}
	}
	err = makeTarAndClean()
	if err != nil {
		panic(err.Error())
	}
}

func makeTarAndClean() error {
	var buf bytes.Buffer
	err := compress(rootOutputDir, &buf)
	if err != nil {
		return err
	}
	// write the .tar.gzip
	fileToWrite, err := os.OpenFile("./scraperlogs.tar.gzip", os.O_CREATE|os.O_RDWR, os.FileMode(600))
	if err != nil {
		return err
	}
	if _, err := io.Copy(fileToWrite, &buf); err != nil {
		return err
	}
	err = os.RemoveAll(rootOutputDir)
	if err != nil {
		return err
	}
	return nil
}

func captureEvents(ns string, clientset *kubernetes.Clientset) error {
	eventList, err := clientset.CoreV1().Events(ns).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	eventData, err := json.MarshalIndent(eventList, "", "	")
	if err != nil {
		return err
	}

	fileName := filepath.Join(eventlogsDirectory, ns+"-events")
	err = populateScraperDir(eventData, fileName)
	if err != nil {
		return err
	}
	return nil
}

func capturePVCLogs(ns string, clientset *kubernetes.Clientset) error {
	pvcList, err := clientset.CoreV1().PersistentVolumeClaims(ns).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	for pvcIndex := range pvcList.Items {
		pvcData, err := json.MarshalIndent(pvcList.Items[pvcIndex], "", "	")
		if err != nil {
			return err
		}

		fileName := filepath.Join(describeDirectoryPVC, ns+"-"+pvcList.Items[pvcIndex].Name)
		err = populateScraperDir(pvcData, fileName)
		if err != nil {
			return err
		}
	}
	return nil
}

func captureAeroclusterLogs(ns string, k8sClient client.Client) error {
	listOps := &client.ListOptions{
		Namespace: ns,
	}
	list := &asdbv1beta1.AerospikeClusterList{}
	err := k8sClient.List(context.TODO(), list, listOps)
	if err != nil {
		return err
	}
	for clusterIndex := range list.Items {
		clusterData, err := json.MarshalIndent(list.Items[clusterIndex], "", "	")
		if err != nil {
			return err
		}
		fileName := filepath.Join(describeDirectoryAero, ns+"-"+list.Items[clusterIndex].Name)
		err = populateScraperDir(clusterData, fileName)
		if err != nil {
			return err
		}
	}
	return nil
}

func captureSTSLogs(ns string, clientset *kubernetes.Clientset) error {
	stsList, err := clientset.AppsV1().StatefulSets(ns).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	for stsIndex := range stsList.Items {
		stsData, err := json.MarshalIndent(stsList.Items[stsIndex], "", "	")
		if err != nil {
			return err
		}

		fileName := filepath.Join(describeDirectorySTS, ns+"-"+stsList.Items[stsIndex].Name)
		err = populateScraperDir(stsData, fileName)
		if err != nil {
			return err
		}
	}
	return nil
}

func capturePodLogs(ns string, clientset *kubernetes.Clientset) error {
	pods, err := clientset.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	for podIndex := range pods.Items {
		podData, err := json.MarshalIndent(pods.Items[podIndex], "", "	")
		if err != nil {
			return err
		}

		fileName := filepath.Join(logsDirectoryPod, "..", ns+"-"+pods.Items[podIndex].Name)
		err = populateScraperDir(podData, fileName)
		if err != nil {
			return err
		}

		for containerIndex := range pods.Items[podIndex].Spec.Containers {
			containerName := pods.Items[podIndex].Spec.Containers[containerIndex].Name
			podLogOpts := corev1.PodLogOptions{Container: containerName}
			req := clientset.CoreV1().Pods(ns).GetLogs(pods.Items[podIndex].Name, &podLogOpts)
			podLogs, err := req.Stream(context.TODO())
			if err != nil {
				return err
			}
			defer podLogs.Close()

			buf := new(bytes.Buffer)
			_, err = io.Copy(buf, podLogs)
			if err != nil {
				return err
			}
			fileName := filepath.Join(logsDirectoryPod, ns+"-"+pods.Items[podIndex].Name+"-"+containerName+"-current.log")
			err = populateScraperDir(buf.Bytes(), fileName)
			if err != nil {
				return err
			}
		}

		for initContainerIndex := range pods.Items[podIndex].Spec.InitContainers {
			initContainerName := pods.Items[podIndex].Spec.InitContainers[initContainerIndex].Name
			podLogOpts := corev1.PodLogOptions{Container: initContainerName}
			req := clientset.CoreV1().Pods(ns).GetLogs(pods.Items[podIndex].Name, &podLogOpts)
			podLogs, err := req.Stream(context.TODO())
			if err != nil {
				return err
			}
			defer podLogs.Close()

			buf := new(bytes.Buffer)
			_, err = io.Copy(buf, podLogs)
			if err != nil {
				return err
			}
			fileName := filepath.Join(logsDirectoryPod, ns+"-"+pods.Items[podIndex].Name+"-"+initContainerName+"-current.log")
			err = populateScraperDir(buf.Bytes(), fileName)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func createDirStructure() error {
	err := os.MkdirAll(logsDirectoryPod, os.ModePerm)
	if err != nil {
		return err
	}
	err = os.MkdirAll(eventlogsDirectory, os.ModePerm)
	if err != nil {
		return err
	}
	err = os.MkdirAll(describeDirectorySTS, os.ModePerm)
	if err != nil {
		return err
	}
	err = os.MkdirAll(describeDirectoryAero, os.ModePerm)
	if err != nil {
		return err
	}
	err = os.MkdirAll(describeDirectoryPVC, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func populateScraperDir(data []byte, fileName string) error {
	filePtr, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	bufferedWriter := bufio.NewWriter(filePtr)

	_, err = bufferedWriter.Write(data)
	if err != nil {
		return err
	}
	bufferedWriter.Flush()
	filePtr.Close()
	return nil
}

func compress(src string, buf io.Writer) error {
	// tar > gzip > buf
	zr := gzip.NewWriter(buf)
	tw := tar.NewWriter(zr)

	// walk through every file in the folder
	filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {
		// generate tar header
		header, err := tar.FileInfoHeader(fi, file)
		if err != nil {
			return err
		}

		// must provide real name
		// (see https://golang.org/src/archive/tar/common.go?#L626)
		header.Name = filepath.ToSlash(file)

		// write header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		// if not a dir, write file content
		if !fi.IsDir() {
			data, err := os.Open(file)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, data); err != nil {
				return err
			}
		}
		return nil
	})

	// produce tar
	if err := tw.Close(); err != nil {
		return err
	}
	// produce gzip
	if err := zr.Close(); err != nil {
		return err
	}
	//
	return nil
}
