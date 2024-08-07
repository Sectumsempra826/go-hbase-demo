package main

import (
	"context"
	pb "go-hbase-demo/cloudpb"
	"reflect"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"google.golang.org/protobuf/proto"
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

// 定义模拟的 HBase 客户端
type MockHBaseClient struct {
	mock.Mock
}

// 实现所有 gohbase.Client 接口方法
func (m *MockHBaseClient) Append(app *hrpc.Mutate) (*hrpc.Result, error) {
	args := m.Called(app)
	return args.Get(0).(*hrpc.Result), args.Error(1)
}

func (m *MockHBaseClient) CheckAndPut(put *hrpc.Mutate, family string, qualifier string, expectedValue []byte) (bool, error) {
	args := m.Called(put, family, qualifier, expectedValue)
	return args.Bool(0), args.Error(1)
}

func (m *MockHBaseClient) Close() {
	m.Called()
}

func (m *MockHBaseClient) Delete(del *hrpc.Mutate) (*hrpc.Result, error) {
	args := m.Called(del)
	return args.Get(0).(*hrpc.Result), args.Error(1)
}

func (m *MockHBaseClient) Get(get *hrpc.Get) (*hrpc.Result, error) {
	args := m.Called(get)
	return args.Get(0).(*hrpc.Result), args.Error(1)
}

func (m *MockHBaseClient) Increment(inc *hrpc.Mutate) (int64, error) {
	args := m.Called(inc)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockHBaseClient) MutateRow(mr *hrpc.Mutate) (*hrpc.Result, error) {
	args := m.Called(mr)
	return args.Get(0).(*hrpc.Result), args.Error(1)
}

func (m *MockHBaseClient) Put(put *hrpc.Mutate) (*hrpc.Result, error) {
	args := m.Called(put)
	return args.Get(0).(*hrpc.Result), args.Error(1)
}

func (m *MockHBaseClient) Scan(scan *hrpc.Scan) hrpc.Scanner {
	args := m.Called(scan)
	return args.Get(0).(hrpc.Scanner)
}

// 测试 Put 方法
func TestPut(t *testing.T) {
	mockClient := new(MockHBaseClient) // 创建一个 MockHBaseClient 实例
	s := &server{client: mockClient}   // 使用 MockHBaseClient 创建 server 实例

	// 模拟 SeqItem，包含多个项
	seqItems := &pb.SeqItems{
		Items: []*pb.SeqItem{
			{
				Key: &pb.SeqKey{
					BizId: []byte("biz1"),
					Seq:   1,
				},
				Value: []byte("value1"),
			},
			{
				Key: &pb.SeqKey{
					BizId: []byte("biz1"),
					Seq:   2,
				},
				Value: []byte("value2"),
			},
			{
				Key: &pb.SeqKey{
					BizId: []byte("biz1"),
					Seq:   3,
				},
				Value: []byte("value3"),
			},
			{
				Key: &pb.SeqKey{
					BizId: []byte("biz1"),
					Seq:   4,
				},
				Value: []byte("value4"),
			},
		},
	}

	// 序列化 SeqItem 的数据
	data1, err := proto.Marshal(seqItems.Items[0])
	if err != nil {
		t.Fatalf("Failed to marshal SeqItem: %v", err)
	}
	data2, err := proto.Marshal(seqItems.Items[1])
	if err != nil {
		t.Fatalf("Failed to marshal SeqItem: %v", err)
	}

	// 设置模拟的行为，确保 Put 方法被调用并返回预期的结果
	mockClient.On("Put", mock.Anything).Return(&hrpc.Result{}, nil).Run(func(args mock.Arguments) {
		put := args.Get(0).(*hrpc.Mutate) // 获取传入的 hrpc.Mutate 对象
		expectedRowKey := generateRowKey("biz1", 1)
		if string(put.Key()) != expectedRowKey {
			t.Errorf("Expected row key %v, got %v", expectedRowKey, string(put.Key()))
		}
		expectedData := map[string]map[string][]byte{
			"cf": {"value": data1},
		}
		if !comparePutData(put, expectedData) { // 比较实际数据与预期数据
			t.Errorf("Expected data %v, got %v", expectedData, put.Values())
		}
	}).Once() // 确保此模拟行为仅执行一次

	mockClient.On("Put", mock.Anything).Return(&hrpc.Result{}, nil).Run(func(args mock.Arguments) {
		put := args.Get(0).(*hrpc.Mutate) // 获取传入的 hrpc.Mutate 对象
		expectedRowKey := generateRowKey("biz2", 2)
		if string(put.Key()) != expectedRowKey {
			t.Errorf("Expected row key %v, got %v", expectedRowKey, string(put.Key()))
		}
		expectedData := map[string]map[string][]byte{
			"cf": {"value": data2},
		}
		if !comparePutData(put, expectedData) { // 比较实际数据与预期数据
			t.Errorf("Expected data %v, got %v", expectedData, put.Values())
		}
	}).Once()

	// 调用被测试的 Put 方法
	_, err = s.Put(context.Background(), seqItems)
	if err != nil {
		t.Fatalf("Put method failed: %v", err)
	}

	mockClient.AssertExpectations(t) // 确保所有预期的模拟行为都已被调用
}

// comparePutData 比较 HBase Put 请求的数据
func comparePutData(put *hrpc.Mutate, expectedData map[string]map[string][]byte) bool {
	for family, cols := range expectedData { // 遍历每个列族
		for col, val := range cols { // 遍历每个列限定符
			if string(put.Values()[family][col]) != string(val) { // 比较实际值与预期值
				return false
			}
		}
	}
	return true
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
