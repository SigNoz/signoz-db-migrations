package main

import (
	"testing"
)

func Test_parseTTL(t *testing.T) {
	type args struct {
		queryResp string
	}
	tests := []struct {
		name            string
		args            args
		wantDelTTL      int
		wantMoveTTL     int
		coldStorageName string
		wantErr         bool
	}{
		{
			name: "Test_parseTTL",
			args: args{
				queryResp: `TTL toDateTime(timestamp / 1000000000) + toIntervalSecond(1296000) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`,
			},
			wantDelTTL:  1296000,
			wantMoveTTL: -1,
			wantErr:     false,
		},
		{
			name: "Test_parseTTL with move",
			args: args{
				queryResp: `TTL toDateTime(timestamp / 1000000000) + toIntervalSecond(1296000) DELETE, toDateTime(timestamp / 1000000000) + toIntervalSecond(1728000) TO VOLUME 'cold' SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`,
			},
			wantDelTTL:      1296000,
			wantMoveTTL:     1728000,
			coldStorageName: "cold",
			wantErr:         false,
		},
		{
			name: "Test_parseTTL with different name",
			args: args{
				queryResp: `TTL toDateTime(timestamp / 1000000000) + toIntervalSecond(1296000) DELETE, toDateTime(timestamp / 1000000000) + toIntervalSecond(1728000) TO VOLUME 's3' SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`,
			},
			wantDelTTL:      1296000,
			wantMoveTTL:     1728000,
			coldStorageName: "s3",
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDelTTL, gotMoveTTL, gotColdStorageName, err := parseTTL(tt.args.queryResp)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTTL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotDelTTL != tt.wantDelTTL {
				t.Errorf("parseTTL() gotDelTTL = %v, want %v", gotDelTTL, tt.wantDelTTL)
			}
			if gotMoveTTL != tt.wantMoveTTL {
				t.Errorf("parseTTL() gotMoveTTL = %v, want %v", gotMoveTTL, tt.wantMoveTTL)
			}
			if gotColdStorageName != tt.coldStorageName {
				t.Errorf("parseTTL() gotColdStorageName = %v, want %v", gotMoveTTL, tt.wantMoveTTL)
			}
		})
	}
}
