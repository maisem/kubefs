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
	_ "k8s.io/client-go/plugin/pkg/client/auth"
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
	types     = []struct {
		apiVersion string
		kind       string
	}{
		{
			apiVersion: "apps/v1",
			kind:       "Deployment",
		},
		{
			apiVersion: "v1",
			kind:       "Pod",
		},
		{
			apiVersion: "v1",
			kind:       "Service",
		},
		{
			apiVersion: "apps/v1",
			kind:       "ReplicaSet",
		},
	}
)

func main() {
	flag.Parse()
	m, err := controllers.NewManager(controllers.GetConfigOrDie(), controllers.Options{})
	if err != nil {
		glog.Fatal(err, "could not create manager")
	}
	kfs := kubefs.New(m.GetClient())
	mp := filepath.Clean(*mountPath)
	if err := os.MkdirAll(mp, os.ModeDir|0700); err != nil {
		glog.Fatal(err, "could not create dir")
	}

	for _, t := range types {
		if err := kfs.Register(m, t.apiVersion, t.kind); err != nil {
			glog.Fatal(err, "could not register")
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
