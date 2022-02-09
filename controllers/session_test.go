/*
Copyright 2022 The Kubernetes Authors.

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

package controllers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/vmware/govmomi/simulator"
	_ "github.com/vmware/govmomi/vapi/simulator"
	"k8s.io/klog/v2/klogr"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/cluster-api-provider-vsphere/apis/v1beta1"
	"sigs.k8s.io/cluster-api-provider-vsphere/pkg/session"
	"sigs.k8s.io/cluster-api-provider-vsphere/test/helpers"
)

func TestGetSession(t *testing.T) {
	g := NewWithT(t)
	log := klogr.New()
	ctrllog.SetLogger(log)

	model := simulator.VPX()
	model.Cluster = 2

	simr, err := helpers.VCSimBuilder().
		WithModel(model).Build()
	if err != nil {
		t.Fatalf("failed to create VC simulator")
	}
	defer simr.Destroy()

	params := session.NewParams().
		WithServer(simr.ServerURL().Host).
		WithUserInfo(simr.Username(), simr.Password()).WithDatacenter("*")

	// Get first session
	s, err := session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(s).ToNot(BeNil())
	assertSessionCountEqualTo(g, simr, 1)

	// Get session key
	sessionInfo, err := s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(sessionInfo).ToNot(BeNil())
	firstSession := sessionInfo.Key

	// remove session expect no session
	g.Expect(s.TagManager.Logout(context.Background())).To(Succeed())
	g.Expect(simr.Run(fmt.Sprintf("session.rm %s", firstSession))).To(Succeed())
	assertSessionCountEqualTo(g, simr, 0)

	// request sesion again should be a new and different session
	s, err = session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(s).ToNot(BeNil())

	// Get session info, session key should be different from
	// last session
	sessionInfo, err = s.SessionManager.UserSession(context.Background())
	g.Expect(sessionInfo).ToNot(BeNil())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(sessionInfo.Key).ToNot(BeEquivalentTo(firstSession))
	assertSessionCountEqualTo(g, simr, 1)
}

func sessionCount(stdout io.Reader) (int, error) {
	buf := make([]byte, 1024)
	count := 0
	lineSep := []byte(v1beta1.GroupVersion.String())

	for {
		c, err := stdout.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func assertSessionCountEqualTo(g *WithT, simr *helpers.Simulator, count int) {
	g.Eventually(func() bool {
		stdout := gbytes.NewBuffer()
		g.Expect(simr.Run("session.ls", stdout)).To(Succeed())
		sessions, err := sessionCount(stdout)
		g.Expect(err).ToNot(HaveOccurred())
		return sessions == count
	}, timeout).Should(BeTrue())
}

func TestGetSessionWithKeepAlive(t *testing.T) {
	g := NewWithT(t)
	log := klogr.New()
	ctrllog.SetLogger(log)

	model := simulator.VPX()
	model.Cluster = 2

	simr, err := helpers.VCSimBuilder().
		WithModel(model).Build()
	if err != nil {
		t.Fatalf("failed to create VC simulator")
	}
	defer simr.Destroy()

	params := session.NewParams().
		WithServer(simr.ServerURL().Host).
		WithUserInfo(simr.Username(), simr.Password()).
		WithFeatures(session.Feature{EnableKeepAlive: true}).
		WithDatacenter("*")

	// Get first Session
	s, err := session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(s).ToNot(BeNil())
	assertSessionCountEqualTo(g, simr, 1)

	// Get session key
	sessionInfo, err := s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(sessionInfo).ToNot(BeNil())
	firstSession := sessionInfo.Key

	// Get the session again
	// as keep alive is enabled and session is
	// not expired we must get the same cached session
	s, err = session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(s).ToNot(BeNil())
	sessionInfo, err = s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(sessionInfo).ToNot(BeNil())
	g.Expect(sessionInfo.Key).To(BeEquivalentTo(firstSession))
	assertSessionCountEqualTo(g, simr, 1)

	// Try to remove vim session
	g.Expect(s.Logout(context.Background())).To(Succeed())

	// after logging out old session must be deleted and
	// we must get a new different session
	// total session count must remain 1
	s, err = session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(s).ToNot(BeNil())
	sessionInfo, err = s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(sessionInfo).ToNot(BeNil())
	g.Expect(sessionInfo.Key).ToNot(BeEquivalentTo(firstSession))
	assertSessionCountEqualTo(g, simr, 1)
}

func TestGetSessionWithKeepAliveTagManagerLogout(t *testing.T) {
	g := NewWithT(t)
	log := klogr.New()
	ctrllog.SetLogger(log)

	simulator.SessionIdleTimeout = 200 * time.Millisecond
	model := simulator.VPX()
	model.Cluster = 2

	simr, err := helpers.VCSimBuilder().
		WithModel(model).Build()
	if err != nil {
		t.Fatalf("failed to create VC simulator")
	}
	defer simr.Destroy()

	params := session.NewParams().
		WithServer(simr.ServerURL().Host).
		WithUserInfo(simr.Username(), simr.Password()).
		WithFeatures(session.Feature{EnableKeepAlive: true, KeepAliveDuration: 400 * time.Millisecond}).WithDatacenter("*")

	// Get first session
	s, err := session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(s).ToNot(BeNil())
	assertSessionCountEqualTo(g, simr, 1)
	sessionInfo, err := s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(sessionInfo).ToNot(BeNil())
	sessionKey := sessionInfo.Key
	assertSessionCountEqualTo(g, simr, 1)

	// wait enough time so the session is expired
	// as KeepAliveDuration 2 seconds > SessionIdleTimeout 1 second
	assertSessionCountEqualTo(g, simr, 0)

	// Get session again
	// as session is deleted we must get new session
	// old session is expected to be cleaned up so count == 1
	s, err = session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(s).ToNot(BeNil())
	assertSessionCountEqualTo(g, simr, 1)
	sessionInfo, err = s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(sessionInfo).ToNot(BeNil())
	g.Expect(sessionInfo.Key).ToNot(BeEquivalentTo(sessionKey))
	sessionKey = sessionInfo.Key
	assertSessionCountEqualTo(g, simr, 1)

	// wait enough time so the session is expired
	// as KeepAliveDuration 2 seconds > SessionIdleTimeout 1 second
	assertSessionCountEqualTo(g, simr, 0)

	s, err = session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(s).ToNot(BeNil())
	sessionInfo, err = s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(sessionInfo).ToNot(BeNil())
	g.Expect(sessionInfo.Key).ToNot(BeEquivalentTo(sessionKey))
	assertSessionCountEqualTo(g, simr, 1)
}
