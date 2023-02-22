/*
Copyright 2023 The aerospike-operator Authors.
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

package cmd

import (
	"github.com/spf13/cobra"

	"create-podstatus/pkg"
)

var (
	pod    *string
	ns     *string
	hostIP *string
)

// updatePodStatus represents the updatePodStatus command
var setIpEnv = &cobra.Command{
	Use:   "setIpEnv",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return pkg.SetHostPortEnv(pod, ns, hostIP)
	},
}

func init() {
	rootCmd.AddCommand(setIpEnv)
	pod = setIpEnv.Flags().String("pod-name", "", "name of pod")
	ns = setIpEnv.Flags().String("namespace", "", "namespace")
	hostIP = setIpEnv.Flags().String("host-ip", "", "host ip")
}
