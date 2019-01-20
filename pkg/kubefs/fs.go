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

package kubefs

import (
	"context"
	"fmt"
	"path"
	"sync"
	"sync/atomic"

	"bazil.org/fuse/fs"
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	controllers "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func New(c client.Client) *KubeFS {
	x := &KubeFS{
		client: c,
	}
	x.root = newDir(c, x.newInode)
	return x
}

type KubeFS struct {
	sync.RWMutex
	client       client.Client
	root         *dir
	inodeCounter uint64
}

func (fs *KubeFS) newInode() uint64 {
	return atomic.AddUint64(&fs.inodeCounter, 1)
}

func (fs *KubeFS) Root() (fs.Node, error) {
	return fs.root, nil
}

func (fs *KubeFS) newNode(apiVersion, kind, namespace, name string) *refNode {
	return newNode(
		fs.newInode(),
		corev1.ObjectReference{
			APIVersion: apiVersion,
			Kind:       kind,
			Namespace:  namespace,
			Name:       name,
		},
		fs.client,
	)
}

func (fs *KubeFS) Register(m manager.Manager, apiVersion, kind string) error {
	f := reconcile.Func(func(req reconcile.Request) (reconcile.Result, error) {
		return fs.reconcile(apiVersion, kind, req)
	})
	return controllers.NewControllerManagedBy(m).For(newObject(apiVersion, kind)).Complete(f)
}

func (fs *KubeFS) reconcile(apiVersion, kind string, req controllers.Request) (controllers.Result, error) {
	ctx := context.Background()
	fileName := fmt.Sprintf("%s.yaml", req.Name)
	key := path.Join(apiVersion, kind, req.Namespace, fileName)
	o := newObject(apiVersion, kind)
	if err := fs.client.Get(ctx, req.NamespacedName, o); err != nil {
		if errors.IsNotFound(err) {
			// This means we need to delete the ref.
			return controllers.Result{}, fs.root.remove(ctx, key)
		}
		glog.Error(err)
	}

	var nn *refNode
	if n, err := fs.root.Lookup(ctx, key); err != nil {
		nn = fs.newNode(apiVersion, kind, req.Namespace, req.Name)
		if err := fs.root.add(key, nn); err != nil {
			return controllers.Result{}, err
		}
	} else {
		var ok bool
		nn, ok = n.(*refNode)
		if !ok {
			return controllers.Result{}, fmt.Errorf("unknown type")
		}
	}

	if err := nn.Update(o); err != nil {
		return controllers.Result{}, err
	}
	return controllers.Result{}, nil
}

func newObject(apiVersion, kind string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
		},
	}
}
