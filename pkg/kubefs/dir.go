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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newDir(c client.Client, newInode func() uint64) *dir {
	return &dir{
		client:   c,
		dir:      make(map[string]fs.Node),
		newInode: newInode,
	}
}

type dir struct {
	sync.RWMutex
	client   client.Client
	dir      map[string]fs.Node
	ref      corev1.ObjectReference
	newInode func() uint64
}

func (d *dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if req.Dir {
		return fuse.EPERM
	}
	x, err := d.Lookup(ctx, req.Name)
	if err == fuse.ENOENT {
		return nil
	}
	if err != nil {
		return err
	}

	rn, ok := x.(*refNode)
	if !ok {
		return fuse.EPERM
	}
	if err := rn.Delete(ctx); err != nil {
		return err
	}
	return d.remove(ctx, req.Name)

}

func (d *dir) remove(ctx context.Context, p string) error {
	var err error
	defer func() {
		if err != nil {
			glog.Errorf("failed to remove %q: %v", p, err)
		}
	}()
	if len(p) == 0 {
		return nil
	}
	if filepath.IsAbs(p) {
		p = p[1:]
	}
	p = filepath.Clean(p)
	if p == "." || p == "/" {
		return nil
	}
	d.Lock()
	defer d.Unlock()
	splitP := strings.SplitN(p, "/", 2)
	// This will always be >1 as we have already handled the nil case.
	switch len(splitP) {
	case 1:
		// Path is in the current directory.
		if _, ok := d.dir[p]; ok {
			delete(d.dir, p)
			return nil
		}
	case 2:
		folder, sub := splitP[0], splitP[1]
		// Path is in a nested directory.
		n, ok := d.dir[folder]
		if !ok {
			return nil
		}
		d, ok := n.(*dir)
		if !ok {
			return fuse.Errno(syscall.ENOTDIR)
		}
		return d.remove(ctx, sub)
	}
	return nil
}

func (d *dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	if _, err := d.Lookup(ctx, req.Name); err == nil {
		return nil, nil, fuse.EEXIST
	}
	rn, err := d.newNode(req.Name)
	if err != nil {
		glog.Infof("newNode %v failed: %v", req.Name, err)
		return nil, nil, err
	}
	if err := d.add(req.Name, rn); err != nil {
		glog.Infof("add %v failed: %v", req.Name, err)
		return nil, nil, err
	}
	glog.Infof("Create %v", req.Name)
	os := &fuse.OpenResponse{}
	h, err := rn.Open(ctx, &fuse.OpenRequest{
		Flags:  req.Flags,
		Header: req.Header,
	}, os)
	if err != nil {
		return nil, nil, err
	}
	glog.Infof("Create %v succeeded", req.Name)
	resp.OpenResponse = *os
	resp.Handle = os.Handle
	return rn, h, nil
}

func (d *dir) newNode(name string) (*refNode, error) {
	if !strings.HasSuffix(name, ".yaml") {
		glog.Warningf("newNode %v: unsupported extension", name)
		return nil, fuse.EPERM
	}
	glog.Infof("newNode %v", name)
	ref := d.ref
	ref.Name = name[:len(name)-5]
	x := newObject(ref.APIVersion, ref.Kind)
	x.SetNamespace(ref.Namespace)
	x.SetName(ref.Name)
	rn := newNode(d.newInode(), ref, d.client)
	if err := rn.Update(x); err != nil {
		return nil, err
	}
	rn.exists = false
	return rn, nil
}

func (d *dir) add(p string, newNode *refNode) (err error) {
	defer func() {
		if err != nil {
			glog.Errorf("failed to add %q: %v", p, err)
		}
	}()
	if len(p) == 0 {
		return nil
	}
	if filepath.IsAbs(p) {
		p = p[1:]
	}
	p = filepath.Clean(p)
	if p == "." || p == "/" {
		return nil
	}
	d.Lock()
	defer d.Unlock()
	splitP := strings.SplitN(p, "/", 2)
	// This will always be >1 as we have already handled the nil case.
	switch len(splitP) {
	case 1:
		glog.Infof("creating resource: %v", newNode.ref)
		// Path is in the current directory.
		if _, ok := d.dir[p]; ok {
			return fuse.EEXIST
		}
		d.dir[p] = newNode
	case 2:
		folder, sub := splitP[0], splitP[1]
		// Path is in a nested directory.
		n, ok := d.dir[folder]
		if !ok {
			nd := newDir(d.client, d.newInode)
			glog.Infof("creating dir: %v", folder)
			nd.ref = newNode.ref
			nd.ref.Name = ""
			d.dir[folder] = nd
			n = nd
		}
		d, ok := n.(*dir)
		if !ok {
			return fuse.Errno(syscall.ENOTDIR)
		}
		return d.add(sub, newNode)
	}
	return nil
}

func (d *dir) Attr(_ context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

func (d *dir) Lookup(ctx context.Context, p string) (fs.Node, error) {
	d.RLock()
	defer d.RUnlock()
	if filepath.IsAbs(p) {
		p = p[1:]
	}
	p = filepath.Clean(p)
	if p == "." || p == "" {
		return d, nil
	}
	splitP := strings.SplitN(p, "/", 2)
	// This will always be >1 as we have already handled the nil case.
	switch len(splitP) {
	case 1:
		// Path is in the current directory.
		if n, ok := d.dir[p]; ok {
			return n, nil
		}
	case 2:
		folder, sub := splitP[0], splitP[1]
		// Path is in a nested directory.
		n, ok := d.dir[folder]
		if !ok {
			return nil, fuse.ENOENT
		}
		d, ok := n.(*dir)
		if !ok {
			return nil, fuse.ENOENT
		}
		return d.Lookup(ctx, sub)
	}

	return nil, fuse.ENOENT
}

func (d *dir) ReadDirAll(_ context.Context) ([]fuse.Dirent, error) {
	var out []fuse.Dirent
	for k := range d.dir {
		out = append(out, fuse.Dirent{Name: k})
	}
	return out, nil
}
