package health

import "testing"

type fakeService struct {
	name string
	ok   bool
	msg  string
}

func (f fakeService) ServiceName() string { return f.name }
func (f fakeService) Ok() (bool, string)  { return f.ok, f.msg }

func TestOverallStatus_EmptyRegister_ReturnsTrue(t *testing.T) {
	hr := NewHealthServiceRegister()
	if got := hr.OverallStatus(); got != true {
		t.Fatalf(" OverallStatus() = %v, want true for empty register", got)
	}
}

func TestOverallStatus_AllHealthy_ReturnsTrue(t *testing.T) {
	hr := NewHealthServiceRegister()
	hr.Register(
		fakeService{name: "svc1", ok: true, msg: "ok"},
		fakeService{name: "svc2", ok: true, msg: "ok"},
	)
	if got := hr.OverallStatus(); got != true {
		t.Fatalf(" OverallStatus() = %v, want true when all services healthy", got)
	}
}

func TestOverallStatus_AnyUnhealthy_ReturnsFalse(t *testing.T) {
	hr := NewHealthServiceRegister()
	hr.Register(
		fakeService{name: "svc1", ok: true, msg: "ok"},
		fakeService{name: "svc2", ok: false, msg: "down"},
		fakeService{name: "svc3", ok: true, msg: "ok"},
	)
	if got := hr.OverallStatus(); got != false {
		t.Fatalf(" OverallStatus() = %v, want false when any service is unhealthy", got)
	}
}

func TestCheckAll_EmptyRegister_ReturnsEmptyMap(t *testing.T) {
	hr := NewHealthServiceRegister()
	got := hr.CheckAll()
	if len(got) != 0 {
		t.Fatalf("CheckAll() = %v, want empty map for empty register", got)
	}
}

func TestCheckAll_SingleService_ReturnsCorrectEntry(t *testing.T) {
	hr := NewHealthServiceRegister()
	hr.Register(fakeService{name: "svc1", ok: true, msg: "all good"})

	results := hr.CheckAll()
	entry, found := results["svc1"]
	if !found {
		t.Fatalf("missing entry for svc1: %v", results)
	}

	healthy, ok := entry["healthy"].(bool)
	if !ok || healthy != true {
		t.Fatalf("svc1 healthy = %v (type ok=%v), want true", entry["healthy"], ok)
	}

	msg, ok := entry["message"].(string)
	if !ok || msg != "all good" {
		t.Fatalf("svc1 message = %v (type ok=%v), want %q", entry["message"], ok, "all good")
	}
}

func TestCheckAll_MultipleServices_ReturnsAllEntries(t *testing.T) {
	hr := NewHealthServiceRegister()
	svcs := []fakeService{
		{name: "svcA", ok: true, msg: "up"},
		{name: "svcB", ok: false, msg: "down"},
		{name: "svcC", ok: true, msg: "ok"},
	}
	hr.Register(svcs[0], svcs[1], svcs[2])

	results := hr.CheckAll()
	if len(results) != len(svcs) {
		t.Fatalf("CheckAll() returned %d entries, want %d", len(results), len(svcs))
	}

	for _, s := range svcs {
		entry, found := results[s.name]
		if !found {
			t.Fatalf("missing entry for %s", s.name)
		}
		healthy, ok := entry["healthy"].(bool)
		if !ok || healthy != s.ok {
			t.Fatalf("%s healthy = %v (type ok=%v), want %v", s.name, entry["healthy"], ok, s.ok)
		}
		msg, ok := entry["message"].(string)
		if !ok || msg != s.msg {
			t.Fatalf("%s message = %v (type ok=%v), want %q", s.name, entry["message"], ok, s.msg)
		}
	}
}

func TestIntegration_RegisterCheckAllAndOverall(t *testing.T) {
	hr := NewHealthServiceRegister()

	// Simulate multiple subsystems in a realistic flow
	db := fakeService{name: "database", ok: true, msg: "connected"}
	cache := fakeService{name: "cache", ok: true, msg: "warm"}
	api := fakeService{name: "api", ok: false, msg: "timeout"}

	hr.Register(db, cache, api)

	// Step 1: Check-all output must contain all three services
	results := hr.CheckAll()
	if len(results) != 3 {
		t.Fatalf("expected 3 registered services, got %d", len(results))
	}

	// Validate each result
	tests := []fakeService{db, cache, api}
	for _, svc := range tests {
		entry, ok := results[svc.name]
		if !ok {
			t.Fatalf("missing entry for service %q", svc.name)
		}

		h, okH := entry["healthy"].(bool)
		if !okH || h != svc.ok {
			t.Fatalf("service %q healthy = %v, want %v", svc.name, entry["healthy"], svc.ok)
		}

		m, okM := entry["message"].(string)
		if !okM || m != svc.msg {
			t.Fatalf("service %q message = %q, want %q", svc.name, m, svc.msg)
		}
	}

	// Step 2: Integration-level verification of the combined system health
	if hr.OverallStatus() != false {
		t.Fatalf("OverallStatus = true, want false when at least one service fails")
	}
}
