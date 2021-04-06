package util

import (
	"encoding/json"
	"fmt"
	"github.com/coreos/ignition/config/util"
	ignitionTypes "github.com/coreos/ignition/config/v2_3/types"
	"github.com/pkg/errors"
	infrav1 "sigs.k8s.io/cluster-api-provider-vsphere/api/v1alpha3"
)

const (
	hostNamePath   = "/etc/hostname"
	rootFileSystem = "root"
)

func ConverBootstrapDatatoIgnition(data []byte) (*ignitionTypes.Config, error) {
	config := &ignitionTypes.Config{}
	if err := json.Unmarshal(data, config); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal bootstrap data into ingition type")
	}
	return config, nil
}

func setHostName(hostname string, config *ignitionTypes.Config) *ignitionTypes.Config {
	for _, file := range config.Storage.Files {
		if file.Path == hostNamePath {
			return config
		}
	}

	// if not found we must set the hostname
	config.Storage.Files = append(config.Storage.Files, ignitionTypes.File{
		Node: ignitionTypes.Node{
			Filesystem: rootFileSystem,
			Path:       hostNamePath,
		},
		FileEmbedded1: ignitionTypes.FileEmbedded1{
			Append: false,
			Contents: ignitionTypes.FileContents{
				Source: fmt.Sprintf("data:,%s", hostname),
			},
			Mode: util.IntToPtr(420),
		},
	})
	return config
}

func setNetowrk(devices []infrav1.NetworkDeviceSpec, config *ignitionTypes.Config) *ignitionTypes.Config {
	ip4 := ""
	for _, device := range devices {
		if len(device.IPAddrs) > 0 {
			ip4 = device.IPAddrs[0]
		}
	}

	if len(config.Networkd.Units) == 0 {
		config.Networkd.Units = append(config.Networkd.Units, ignitionTypes.Networkdunit{
			Contents: fmt.Sprintf("[Match]\nName=ens12\n\n[Network]\nAddress=%s", ip4),
			Name:     "00-ens12.network",
		})
	}

	return config
}