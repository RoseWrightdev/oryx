package mesh

import "testing"

func BenchmarkNopMesh_GetOwners(b *testing.B) {
	mesh := &NopMesh{}
	b.ReportAllocs()
	for b.Loop() {
		_ = mesh.GetOwners("key", 3)
	}
}

func BenchmarkNopMesh_Members(b *testing.B) {
	mesh := &NopMesh{}
	b.ReportAllocs()
	for b.Loop() {
		_ = mesh.Members()
	}
}
