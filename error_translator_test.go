package duckdb

import (
	"errors"
	"testing"
)

func TestDialector_Translate(t *testing.T) {
	type fields struct {
		Config *Config
	}
	type args struct {
		err error
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   error
	}{
		{
			name: "it should return original error for DuckDB errors",
			args: args{err: errors.New("duckdb error")},
			want: errors.New("duckdb error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialector := Dialector{
				Config: tt.fields.Config,
			}
			if err := dialector.Translate(tt.args.err); err.Error() != tt.want.Error() {
				t.Errorf("Translate() expected error = %v, got error %v", tt.want, err)
			}
		})
	}
}
