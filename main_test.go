package main

import (
	"context"
	pb "go-hbase-demo/cloudpb"
	"reflect"
	"testing"

	"github.com/tsuna/gohbase"
)

func Test_hash(t *testing.T) {
	type args struct {
		fileID string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
		{
			name: "Test case 1 - normal case",
			args: args{
				fileID: "file123",
			},
			want: "3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hash(tt.args.fileID); got != tt.want {
				t.Errorf("hash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_generateRowKey(t *testing.T) {
	type args struct {
		fileID   string
		revision int32
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generateRowKey(tt.args.fileID, tt.args.revision); got != tt.want {
				t.Errorf("generateRowKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_server_Put(t *testing.T) {
	type fields struct {
		UnimplementedSeqDbServer pb.UnimplementedSeqDbServer
		client                   gohbase.Client
	}
	type args struct {
		ctx      context.Context
		seqItems *pb.SeqItems
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb.PutItemResp
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &server{
				UnimplementedSeqDbServer: tt.fields.UnimplementedSeqDbServer,
				client:                   tt.fields.client,
			}
			got, err := s.Put(tt.args.ctx, tt.args.seqItems)
			if (err != nil) != tt.wantErr {
				t.Errorf("server.Put() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("server.Put() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_server_Get(t *testing.T) {
	type fields struct {
		UnimplementedSeqDbServer pb.UnimplementedSeqDbServer
		client                   gohbase.Client
	}
	type args struct {
		ctx    context.Context
		seqKey *pb.SeqKey
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb.SeqItem
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &server{
				UnimplementedSeqDbServer: tt.fields.UnimplementedSeqDbServer,
				client:                   tt.fields.client,
			}
			got, err := s.Get(tt.args.ctx, tt.args.seqKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("server.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("server.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_server_QueryRange(t *testing.T) {
	type fields struct {
		UnimplementedSeqDbServer pb.UnimplementedSeqDbServer
		client                   gohbase.Client
	}
	type args struct {
		ctx context.Context
		req *pb.RangeReq
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb.SeqItems
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &server{
				UnimplementedSeqDbServer: tt.fields.UnimplementedSeqDbServer,
				client:                   tt.fields.client,
			}
			got, err := s.QueryRange(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("server.QueryRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("server.QueryRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_server_DeleteRange(t *testing.T) {
	type fields struct {
		UnimplementedSeqDbServer pb.UnimplementedSeqDbServer
		client                   gohbase.Client
	}
	type args struct {
		ctx context.Context
		req *pb.RangeReq
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb.DelRangeResp
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &server{
				UnimplementedSeqDbServer: tt.fields.UnimplementedSeqDbServer,
				client:                   tt.fields.client,
			}
			got, err := s.DeleteRange(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("server.DeleteRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("server.DeleteRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_server_GetMaxKey(t *testing.T) {
	type fields struct {
		UnimplementedSeqDbServer pb.UnimplementedSeqDbServer
		client                   gohbase.Client
	}
	type args struct {
		ctx    context.Context
		seqKey *pb.SeqKey
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb.SeqKey
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &server{
				UnimplementedSeqDbServer: tt.fields.UnimplementedSeqDbServer,
				client:                   tt.fields.client,
			}
			got, err := s.GetMaxKey(tt.args.ctx, tt.args.seqKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("server.GetMaxKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("server.GetMaxKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
