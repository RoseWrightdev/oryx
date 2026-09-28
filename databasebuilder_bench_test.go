package oryx

import "testing"

func BenchmarkDatabaseBuilder_Build(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		builder := NewDatabaseBuilder().
			Default().
			SingleNode().
			SetInsecure()
		_, _ = builder.Build()
	}
}
