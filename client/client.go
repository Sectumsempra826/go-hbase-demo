package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "go-hbase-demo/cloudpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 连接到 gRPC 服务器
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewSeqDbClient(conn)

	// 测试 Put 方法
	putReq := &pb.SeqItems{
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

	_, err = client.Put(context.Background(), putReq)
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Println("Put request successful")

	// 短暂等待，确保数据写入 HBase
	time.Sleep(2 * time.Second)

	// 测试 Get 方法
	getReq := &pb.SeqKey{
		BizId: []byte("biz1"),
		Seq:   1,
	}

	getResp, err := client.Get(context.Background(), getReq)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Get request successful: %v\n", getResp)

	// 测试 GetMaxKey 方法
	getMaxKeyReq := &pb.SeqKey{
		BizId: []byte("biz1"),
		Seq:   1,
	}

	getMaxKeyResp, err := client.GetMaxKey(context.Background(), getMaxKeyReq)
	if err != nil {
		log.Fatalf("GetMaxKey failed: %v", err)
	}
	fmt.Printf("GetMaxKey request successful: %v\n", getMaxKeyResp)

	// 测试 QueryRange 方法
	queryRangeReq := &pb.RangeReq{
		End: &pb.SeqKey{
			BizId: []byte("biz1"),
			Seq:   2,
		},
		Start: &pb.SeqKey{
			BizId: []byte("biz1"),
			Seq:   4,
		},
		// Option: pb.RangeOption_WithoutEnd,
	}

	queryRangeResp, err := client.QueryRange(context.Background(), queryRangeReq)
	if err != nil {
		log.Fatalf("QueryRange failed: %v", err)
	}
	fmt.Printf("QueryRange request successful: %v\n", queryRangeResp)

	// 测试 DeleteRange 方法
	deleteRangeReq := &pb.RangeReq{
		End: &pb.SeqKey{
			BizId: []byte("biz1"),
			Seq:   1,
		},
		Start: &pb.SeqKey{
			BizId: []byte("biz1"),
			Seq:   2,
		},
	}

	_, err = client.DeleteRange(context.Background(), deleteRangeReq)
	if err != nil {
		log.Fatalf("DeleteRange failed: %v", err)
	}
	fmt.Println("DeleteRange request successful")

	// 再次测试 QueryRange 方法，确认数据已删除
	queryRangeResp, err = client.QueryRange(context.Background(), queryRangeReq)
	if err != nil {
		log.Fatalf("QueryRange after delete failed: %v", err)
	}
	fmt.Printf("QueryRange after delete request successful: %v\n", queryRangeResp)
}
