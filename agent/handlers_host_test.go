package agent

import (
	"testing"
)

func TestParseHostContainerLine_JettyManaged(t *testing.T) {
	line := "abcdef1234567890fffff\tjetty_nginx-web-1\tnginx:latest\trunning\tUp 2 hours\t0.0.0.0:80->80/tcp\t2026-04-29 12:00:00 -0400 EDT\tjetty_nginx\tweb"
	hc, ok := parseHostContainerLine(line)
	if !ok {
		t.Fatal("parse failed")
	}
	if hc.ID != "abcdef123456" {
		t.Errorf("ID truncation: got %q, want abcdef123456", hc.ID)
	}
	if !hc.ManagedByJetty {
		t.Error("expected ManagedByJetty=true for jetty_ prefix")
	}
	if hc.WorkloadName != "nginx" {
		t.Errorf("WorkloadName: got %q, want nginx", hc.WorkloadName)
	}
	if hc.ComposeService != "web" {
		t.Errorf("ComposeService: got %q, want web", hc.ComposeService)
	}
	if len(hc.Ports) != 1 || hc.Ports[0] != "0.0.0.0:80->80/tcp" {
		t.Errorf("Ports: got %v", hc.Ports)
	}
}

func TestParseHostContainerLine_Unmanaged(t *testing.T) {
	line := "deadbeefcafe\tcreamer-mailpit\taxllent/mailpit:latest\texited\tExited (0) 12 days ago\t\t2026-04-15 19:42:51 -0400 EDT\tcreamer-20\tmailpit"
	hc, ok := parseHostContainerLine(line)
	if !ok {
		t.Fatal("parse failed")
	}
	if hc.ManagedByJetty {
		t.Error("expected ManagedByJetty=false for non-jetty project")
	}
	if hc.WorkloadName != "" {
		t.Errorf("WorkloadName should be empty for unmanaged, got %q", hc.WorkloadName)
	}
	if hc.ComposeProject != "creamer-20" {
		t.Errorf("ComposeProject: got %q", hc.ComposeProject)
	}
	if hc.State != "exited" {
		t.Errorf("State: got %q", hc.State)
	}
	if len(hc.Ports) != 0 {
		t.Errorf("Ports: got %v, want empty", hc.Ports)
	}
}

func TestParseHostContainerLine_Standalone(t *testing.T) {
	// docker run --name test nginx:latest -- no compose project
	line := "abc123\ttest\tnginx:latest\trunning\tUp\t\t2026-04-29 14:00:00 -0400 EDT\t\t"
	hc, ok := parseHostContainerLine(line)
	if !ok {
		t.Fatal("parse failed")
	}
	if hc.ComposeProject != "" {
		t.Errorf("expected empty ComposeProject, got %q", hc.ComposeProject)
	}
	if hc.ManagedByJetty {
		t.Error("standalone container shouldn't be ManagedByJetty")
	}
}

func TestParseHostContainerLine_MultiplePorts(t *testing.T) {
	line := "abc\tweb\tnginx\trunning\tUp\t0.0.0.0:80->80/tcp, 0.0.0.0:443->443/tcp\t-\tjetty_web\tnginx"
	hc, _ := parseHostContainerLine(line)
	if len(hc.Ports) != 2 {
		t.Fatalf("expected 2 ports, got %v", hc.Ports)
	}
	if hc.Ports[1] != "0.0.0.0:443->443/tcp" {
		t.Errorf("port whitespace not trimmed: %q", hc.Ports[1])
	}
}

func TestParseHostContainerLine_Malformed(t *testing.T) {
	if _, ok := parseHostContainerLine("only\ttwo\tfields"); ok {
		t.Error("should reject under-fielded lines")
	}
	if _, ok := parseHostContainerLine(""); ok {
		t.Error("empty line should be rejected")
	}
}

func TestParseHostContainerLine_NameStripsLeadingSlash(t *testing.T) {
	// Older docker versions / docker inspect output have leading "/" on names.
	line := "abc\t/leading-slash\tnginx\trunning\tUp\t\t-\t\t"
	hc, _ := parseHostContainerLine(line)
	if hc.Name != "leading-slash" {
		t.Errorf("Name: got %q, want leading-slash", hc.Name)
	}
}
