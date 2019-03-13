/*
Copyright 2019 Maisem Ali

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

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/golang/glog"
	"github.com/maisem/kubefs/pkg/kubefs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	controllers "sigs.k8s.io/controller-runtime"
)

func handleInterrupt(f func() error) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)
	go func() {
		<-c
		if err := f(); err != nil {
			log.Fatal(err)
		}
	}()
}

var (
	mountPath = flag.String("path", "fuse", "the mount path for kubefs")
)

type apiResource struct {
	kind       string
	apiVersion string
}

func isWatchable(r *metav1.APIResource) bool {
	var (
		listable  bool
		watchable bool
		gettable  bool
	)

	for _, v := range r.Verbs {
		switch v {
		case "get":
			gettable = true
		case "watch":
			watchable = true
		case "list":
			listable = true
		}
	}
	return listable && watchable && gettable
}

func getAllResources(cfg *rest.Config) []*apiResource {
	dc := discovery.NewDiscoveryClientForConfigOrDie(cfg)
	gl, err := dc.ServerGroups()
	if err != nil {
		glog.Fatalf("could not get list of groups: %v", err)
	}

	var resources []*apiResource
	for _, g := range gl.Groups {
		var gv string
		if g.PreferredVersion.GroupVersion != "" {
			gv = g.PreferredVersion.GroupVersion
		} else if len(g.Versions) > 0 {
			// Just pick the first.
			gv = g.Versions[0].GroupVersion
		} else {
			continue
		}
		resources = append(resources, getResourcesForGroup(dc, gv)...)
	}
	return resources
}

func getResourcesForGroup(dc discovery.DiscoveryInterface, groupVersion string) []*apiResource {
	rl, err := dc.ServerResourcesForGroupVersion(groupVersion)
	if err != nil {
		glog.Fatalf("could not get list of resources for %v: %v", groupVersion, err)
	}
	var resources []*apiResource
	for _, r := range rl.APIResources {
		if !isWatchable(&r) {
			continue
		}
		resources = append(resources, &apiResource{
			apiVersion: groupVersion,
			kind:       r.Kind,
		})
	}
	return resources
}

func main() {
	flag.Parse()
	cfg := controllers.GetConfigOrDie()
	m, err := controllers.NewManager(cfg, controllers.Options{})
	if err != nil {
		glog.Fatal(err, "could not create manager")
	}
	client := m.GetClient()
	kfs := kubefs.New(client)
	mp := filepath.Clean(*mountPath)
	if err := os.MkdirAll(mp, os.ModeDir|0700); err != nil {
		glog.Fatal(err, "could not create dir")
	}

	resources := getAllResources(cfg)
	for _, r := range resources {
		if err := kfs.Register(m, r.apiVersion, r.kind); err != nil {
			glog.Errorf("%s/%s - could not register: %v", r.apiVersion, r.kind, err)
		}
	}

	go func() {
		if err := m.Start(controllers.SetupSignalHandler()); err != nil {
			glog.Fatal(err, "could not start manager")
		}
	}()
	c, err := fuse.Mount(mp, fuse.FSName("kubefs"))
	if err != nil {
		glog.Fatal(err)
	}
	defer c.Close()
	defer fuse.Unmount(mp)
	handleInterrupt(func() error { return fuse.Unmount(mp) })

	err = fs.Serve(c, kfs)
	if err != nil {
		glog.Fatal(err)
	}
	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		glog.Fatal(err)
	}
}
