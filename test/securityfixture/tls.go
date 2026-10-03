// Package securityfixture supplies local certificates and host middleware for
// security contract tests. It is test infrastructure, not Wire security policy.
package securityfixture

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

type (
	// Certificates supplies trusted and unrelated identities for local TLS tests.
	Certificates struct {
		Server, Client, UntrustedClient tls.Certificate
		Roots, OtherRoots               *x509.CertPool
	}
)

// NewCertificates generates private test roots and matching wire.test identities.
func NewCertificates(t testing.TB) Certificates {
	t.Helper()
	root, key := authority(t, 1)
	other, otherKey := authority(t, 2)
	roots, otherRoots := x509.NewCertPool(), x509.NewCertPool()
	roots.AddCert(root)
	otherRoots.AddCert(other)

	return Certificates{
		Server:          issue(t, root, key, 3, x509.ExtKeyUsageServerAuth),
		Client:          issue(t, root, key, 4, x509.ExtKeyUsageClientAuth),
		UntrustedClient: issue(t, other, otherKey, 5, x509.ExtKeyUsageClientAuth),
		Roots:           roots, OtherRoots: otherRoots,
	}
}
func authority(t testing.TB, serial int64) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "Wire test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	return cert, key
}
func issue(t testing.TB, root *x509.Certificate, key *ecdsa.PrivateKey, serial int64, usage x509.ExtKeyUsage) tls.Certificate {
	t.Helper()

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "wire.test"}, DNSNames: []string{"wire.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}

	der, err := x509.CreateCertificate(rand.Reader, template, root, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	return tls.Certificate{Certificate: [][]byte{der, root.Raw}, PrivateKey: leafKey}
}

// ServerConfig requires trusted client certificates when mutual is true.
func (c Certificates) ServerConfig(mutual bool) *tls.Config {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{c.Server}}

	if mutual {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = c.Roots
	}

	return cfg
}

// ClientConfig verifies the server root and wire.test identity without exceptions.
func (c Certificates) ClientConfig(mutual bool) *tls.Config {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: c.Roots, ServerName: "wire.test"}

	if mutual {
		cfg.Certificates = []tls.Certificate{c.Client}
	}

	return cfg
}
