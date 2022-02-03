package controllers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/vmware/govmomi/simulator"
	// run init func to register the tagging API endpoints
	_ "github.com/vmware/govmomi/vapi/simulator"
	"k8s.io/klog/v2/klogr"
	"sigs.k8s.io/cluster-api-provider-vsphere/apis/v1beta1"
	"sigs.k8s.io/cluster-api-provider-vsphere/pkg/session"
	"sigs.k8s.io/cluster-api-provider-vsphere/test/helpers"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

func TestGetSession(t *testing.T) {
	g := gomega.NewWithT(t)
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
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(s).ToNot(gomega.BeNil())
	AssetSessionCountEqualTo(g, simr, 1)

	// Get session key
	sessionInfo, err := s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(sessionInfo).ToNot(gomega.BeNil())
	firstSession := sessionInfo.Key

	// remove session expect no session
	g.Expect(s.TagManager.Logout(context.Background())).To(gomega.Succeed())
	g.Expect(simr.Run(fmt.Sprintf("session.rm %s", firstSession))).To(gomega.Succeed())
	AssetSessionCountEqualTo(g, simr, 0)

	// request sesion again should be a new and different session
	s, err = session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(s).ToNot(gomega.BeNil())

	// Get session info, session key should be different from
	// last session
	sessionInfo, err = s.SessionManager.UserSession(context.Background())
	g.Expect(sessionInfo).ToNot(gomega.BeNil())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(sessionInfo.Key).ToNot(gomega.BeEquivalentTo(firstSession))
	AssetSessionCountEqualTo(g, simr, 1)
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

func AssetSessionCountEqualTo(g *gomega.WithT, simr *helpers.Simulator, count int) {
	stdout := gbytes.NewBuffer()
	g.Expect(simr.Run("session.ls", stdout)).To(gomega.Succeed())
	g.Expect(sessionCount(stdout)).To(gomega.BeNumerically("==", count))
}

func TestGetSessionWithKeepAlive(t *testing.T) {
	g := gomega.NewWithT(t)
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
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(s).ToNot(gomega.BeNil())
	AssetSessionCountEqualTo(g, simr, 1)

	// Get session key
	sessionInfo, err := s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(sessionInfo).ToNot(gomega.BeNil())
	firstSession := sessionInfo.Key

	// Get the session again
	// as keep alive is enabled and session is
	// not expired we must get the same cached session
	s, err = session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(s).ToNot(gomega.BeNil())
	sessionInfo, err = s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(sessionInfo).ToNot(gomega.BeNil())
	g.Expect(sessionInfo.Key).To(gomega.BeEquivalentTo(firstSession))
	AssetSessionCountEqualTo(g, simr, 1)

	// Try to remove vim session
	g.Expect(s.Logout(context.Background())).To(gomega.Succeed())

	// after logging out old session must be deleted and
	// we must get a new different session
	// total session count must remain 1
	s, err = session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(s).ToNot(gomega.BeNil())
	sessionInfo, err = s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(sessionInfo).ToNot(gomega.BeNil())
	g.Expect(sessionInfo.Key).ToNot(gomega.BeEquivalentTo(firstSession))
	AssetSessionCountEqualTo(g, simr, 1)
}

func TestGetSessionWithKeepAliveTagManagerLogout(t *testing.T) {
	g := gomega.NewWithT(t)
	log := klogr.New()
	ctrllog.SetLogger(log)

	simulator.SessionIdleTimeout = 1 * time.Second
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
		WithFeatures(session.Feature{EnableKeepAlive: true, KeepAliveDuration: 2 * time.Second}).WithDatacenter("*")

	// Get first session
	s, err := session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(s).ToNot(gomega.BeNil())
	AssetSessionCountEqualTo(g, simr, 1)
	sessionInfo, err := s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(sessionInfo).ToNot(gomega.BeNil())
	sessionKey := sessionInfo.Key
	AssetSessionCountEqualTo(g, simr, 1)

	// wait enough time so the session is expired
	// as KeepAliveDuration 2 seconds > SessionIdleTimeout 1 second
	time.Sleep(5 * time.Second)
	AssetSessionCountEqualTo(g, simr, 0)

	// Get session again
	// as session is deleted we must get new session
	// old session is expected to be cleaned up so count == 1
	s, err = session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(s).ToNot(gomega.BeNil())
	sessionInfo, err = s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(sessionInfo).ToNot(gomega.BeNil())
	g.Expect(sessionInfo.Key).ToNot(gomega.BeEquivalentTo(sessionKey))
	sessionKey = sessionInfo.Key
	AssetSessionCountEqualTo(g, simr, 1)

	// wait enough time so the session is expired
	// as KeepAliveDuration 2 seconds > SessionIdleTimeout 1 second
	time.Sleep(5 * time.Second)
	AssetSessionCountEqualTo(g, simr, 0)

	s, err = session.GetOrCreate(context.Background(), params)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(s).ToNot(gomega.BeNil())
	sessionInfo, err = s.SessionManager.UserSession(context.Background())
	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(sessionInfo).ToNot(gomega.BeNil())
	g.Expect(sessionInfo.Key).ToNot(gomega.BeEquivalentTo(sessionKey))
	AssetSessionCountEqualTo(g, simr, 1)
}
