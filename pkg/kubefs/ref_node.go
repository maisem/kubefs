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
	"sync"
	"sync/atomic"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

func newNode(inode uint64, ref corev1.ObjectReference, client client.Client) *refNode {
	return &refNode{
		client: client,
		inode:  inode,
		ref:    ref,
		exists: true,
	}
}

type refNode struct {
	sync.RWMutex
	client   client.Client
	inode    uint64
	size     uint64
	refCount uint64
	data     []byte
	buffer   []byte
	cow      bool
	exists   bool
	ref      corev1.ObjectReference
}

func (n *refNode) Delete(ctx context.Context) error {
	n.Lock()
	defer n.Unlock()
	o, err := n.toObject(n.data)
	if err != nil {
		return err
	}
	if err := n.client.Delete(ctx, o); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (n *refNode) get(ctx context.Context) (*unstructured.Unstructured, error) {
	o := newObject(n.ref.APIVersion, n.ref.Kind)
	if err := n.client.Get(ctx, types.NamespacedName{Name: n.ref.Name, Namespace: n.ref.Namespace}, o); err != nil {
		return nil, err
	}
	return o, nil
}

func (n *refNode) createObject(ctx context.Context, o *unstructured.Unstructured) error {
	glog.Infof("Creating %v", n.ref)
	if err := n.client.Create(ctx, o); err != nil {
		glog.Errorf("Failed to create %v: %v", n.ref, err)
		return err
	}
	return nil
}

func (n *refNode) updateObject(ctx context.Context, o *unstructured.Unstructured) error {
	glog.Infof("Updating %v", n.ref)
	if err := n.client.Update(ctx, o); err != nil {
		glog.Errorf("Failed to update %v: %v", n.ref, err)
		return err
	}
	return nil
}

func (n *refNode) Update(x *unstructured.Unstructured) error {
	n.Lock()
	defer n.Unlock()
	b, err := yaml.Marshal(x.Object)
	if err != nil {
		return err
	}
	n.data = b
	n.size = uint64(len(n.data))
	return nil
}

func (n *refNode) toObject(d []byte) (*unstructured.Unstructured, error) {
	o := newObject(n.ref.APIVersion, n.ref.Kind)
	if err := yaml.Unmarshal(d, &o.Object); err != nil {
		glog.Error(err)
		return nil, err
	}
	return o, nil
}

func (n *refNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	n.Lock()
	defer n.Unlock()
	if !n.cow {
		return nil
	}
	glog.Infof("Fsyncing %v", n.ref)
	o, err := n.toObject(n.buffer)
	if err != nil {
		glog.Error(err)
		return err
	}
	if n.exists {
		err = n.updateObject(ctx, o)
	} else {
		err = n.createObject(ctx, o)
	}
	if err != nil {
		glog.Error(err)
		return err
	}
	n.exists = true
	n.buffer = nil
	n.cow = false
	return nil
}

func (n *refNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	n.Lock()
	defer n.Unlock()
	glog.Infof("Opening %v", n.ref)
	hid := fuse.HandleID(atomic.AddUint64(&n.refCount, 1))
	resp.Handle = hid
	return n, nil
}

func (n *refNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	n.Lock()
	defer n.Unlock()
	glog.Infof("Setattr: %v, %v", n.ref, req.Size)
	if (req.Valid & fuse.SetattrSize) != 0 {
		if n.buffer == nil {
			buffer := make([]byte, req.Size)
			copy(buffer, n.data[:req.Size])
			n.buffer = buffer
		} else {
			glog.Infof("setting size")
			n.buffer = n.buffer[:req.Size]
		}
		n.cow = true
	}
	resp.Attr.Size = req.Size
	resp.Attr.Inode = n.inode
	resp.Attr.Mode = 0644
	return nil
}

func (n *refNode) Attr(ctx context.Context, a *fuse.Attr) error {
	n.RLock()
	a.Size = n.size
	n.RUnlock()
	a.Inode = n.inode
	a.Mode = 0644
	return nil
}

func (n *refNode) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	n.RLock()
	defer n.RUnlock()
	glog.Infof("Flushing %v", n.ref)
	_, err := n.toObject(n.buffer)
	return err
}

func (n *refNode) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	n.Lock()
	defer n.Unlock()
	glog.Infof("Writing %v", n.ref)
	if n.buffer == nil {
		buffer := make([]byte, len(n.data))
		copy(buffer, n.data)
		n.buffer = buffer
		n.cow = true
	}
	x := copy(n.buffer[req.Offset:], req.Data)
	if x < len(req.Data) {
		n.buffer = append(n.buffer, req.Data[x:]...)
	}
	resp.Size = len(req.Data)
	return nil
}

func (n *refNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	n.RLock()
	defer n.RUnlock()
	glog.Infof("Reading %v", n.ref)
	start := req.Offset
	end := req.Offset + int64(req.Size)
	if x := int64(len(n.data)); end > x {
		end = x
	}
	resp.Data = n.data[start:end]
	return nil
}
