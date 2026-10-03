// Package server hosts the Ferret Wire protocol through managed TCP serving or
// caller-created listeners, over a borrowed caller-configured Unified API runtime.
// Construction never opens a listener. Host credentials and middleware apply to
// both serving paths; without credentials there is no encryption or authentication.
package server
