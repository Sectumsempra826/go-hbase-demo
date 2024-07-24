package main

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "go-hbase-demo/cloudpb" // 导入生成的 Protobuf 包

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"google.golang.org/grpc"
)

// 计算 file_id 的哈希值:取fileID最后一位
func hash(fileID string) string {
	// h := sha256.New()                     // 创建新的 SHA-256 哈希对象
	// h.Write([]byte(fileID))               // 将 fileID 转换为字节数组并写入哈希对象
	// return hex.EncodeToString(h.Sum(nil)) // 将哈希结果转换为十六进制字符串
	if len(fileID) == 0 {
		return ""
	}
	return string(fileID[len(fileID)-1])
}

// 根据设计生成 RowKey
func generateRowKey(fileID string, revision int32) string {
	fileIDHash := hash(fileID) // 计算 file_id 的哈希值
	// 将 int32 转换为 uint32 并生成 RowKey，使用按位取反（^revision）实现 uint32.Max - revision
	rowKey := fmt.Sprintf("%s_%s_%010d", fileIDHash, fileID, ^uint32(revision))
	return rowKey
}

// 定义 gRPC 服务器结构体
type server struct {
	pb.UnimplementedSeqDbServer                // 嵌入未实现的 gRPC 服务器，提供默认实现
	client                      gohbase.Client // HBase 客户端
}

// 创建新的 gRPC 服务器实例，并连接到 HBase
func NewServer() *server {
	client := gohbase.NewClient("localhost")
	return &server{client: client}
}

// 实现 gRPC 服务的 Put 方法
// TODO 批量插入
// 将接收到的 SeqItems 存储到 HBase 中
func (s *server) Put(ctx context.Context, seqItems *pb.SeqItems) (*pb.PutItemResp, error) {
	for _, item := range seqItems.Items {
		rowKey := generateRowKey(string(item.Key.BizId), item.Key.Seq) // 生成 RowKey
		// 创建 HBase Put 请求
		// HBase Shell中建表：create 'my_table','cf'
		putRequest, err := hrpc.NewPutStr(ctx, "my_table", rowKey, map[string]map[string][]byte{
			"cf": { // 列族
				"value": item.Value, // 列名和值
			},
		})
		if err != nil {
			log.Printf("Put request creation failed: %v", err)
			return nil, err // 返回错误
		}
		_, err = s.client.Put(putRequest) // 执行 Put 请求
		if err != nil {
			log.Printf("Put request execution failed: %v", err)
			return nil, err // 返回错误
		}
	}
	log.Println("Put request successful")
	return &pb.PutItemResp{}, nil // 返回空的响应
}

// 实现 gRPC 服务的 Get 方法
// 根据 SeqKey 从 HBase 中检索数据
func (s *server) Get(ctx context.Context, seqKey *pb.SeqKey) (*pb.SeqItem, error) {
	rowKey := generateRowKey(string(seqKey.BizId), seqKey.Seq) // 生成 RowKey
	// 创建 HBase Get 请求
	getRequest, err := hrpc.NewGetStr(ctx, "my_table", rowKey)
	if err != nil {
		log.Printf("Get request creation failed: %v", err)
		return nil, err // 返回错误
	}
	getRsp, err := s.client.Get(getRequest)
	if err != nil {
		log.Printf("Get request execution failed: %v", err)
		return nil, err // 返回错误
	}
	value := getRsp.Cells[0].Value // 获取返回值
	log.Println("Get request successful")
	return &pb.SeqItem{Key: seqKey, Value: value}, nil // 返回 SeqItem
}

// 实现 gRPC 服务的 GetMaxKey 方法
// 获取最大 SeqKey
func (s *server) GetMaxKey(ctx context.Context, seqKey *pb.SeqKey) (*pb.SeqKey, error) {
	// 这里的实现仅作为示例，实际应用中需要根据具体业务逻辑进行调整
	rowKey := generateRowKey(string(seqKey.BizId), seqKey.Seq) // 生成 RowKey
	getRequest, err := hrpc.NewGetStr(ctx, "my_table", rowKey)
	if err != nil {
		log.Printf("GetMaxKey request creation failed: %v", err)
		return nil, err // 返回错误
	}
	getRsp, err := s.client.Get(getRequest)
	if err != nil {
		log.Printf("GetMaxKey request execution failed: %v", err)
		return nil, err // 返回错误
	}
	if len(getRsp.Cells) == 0 {
		log.Println("GetMaxKey request found no cells")
		return nil, fmt.Errorf("no cells found")
	}
	// 假设返回的第一条记录中的行键就是最大键
	maxSeqKey := &pb.SeqKey{
		BizId: []byte(getRsp.Cells[0].Row),
		Seq:   seqKey.Seq, // 这里假设 Seq 一致，需要根据实际业务逻辑进行调整
	}
	log.Println("GetMaxKey request successful")
	return maxSeqKey, nil // 返回最大 SeqKey
}

// 实现 gRPC 服务的 QueryRange 方法
// 根据范围请求检索 SeqItems
func (s *server) QueryRange(ctx context.Context, req *pb.RangeReq) (*pb.SeqItems, error) {
	startRowKey := generateRowKey(string(req.Start.BizId), req.Start.Seq) // 生成起始 RowKey
	endRowKey := generateRowKey(string(req.End.BizId), req.End.Seq)       // 生成结束 RowKey

	log.Printf("QueryRange startRowKey: %s, endRowKey: %s", startRowKey, endRowKey)

	scanRequest, err := hrpc.NewScanRangeStr(ctx, "my_table", startRowKey, endRowKey)
	if err != nil {
		log.Printf("QueryRange scan request creation failed: %v", err)
		return nil, err // 返回错误
	}
	scanner := s.client.Scan(scanRequest)
	items := []*pb.SeqItem{}
	for {
		res, err := scanner.Next()
		if err != nil {
			log.Printf("QueryRange scanner next failed: %v", err)
			if err.Error() == "EOF" {
				break // 扫描结束
			}
			return nil, err // 返回错误
		}
		if res == nil {
			log.Println("QueryRange scanner reached end of results")
			break // 扫描结束
		}
		for _, cell := range res.Cells {
			log.Printf("QueryRange found cell: %s", cell.Row)
			items = append(items, &pb.SeqItem{
				Key:   &pb.SeqKey{BizId: []byte(cell.Row), Seq: req.Start.Seq}, // 假设 seqKey 一致
				Value: cell.Value,
			})
		}
	}
	log.Println("QueryRange request successful")
	return &pb.SeqItems{Items: items}, nil // 返回 SeqItems
}

// 实现 gRPC 服务的 DeleteRange 方法
// 删除指定范围的 SeqItems
func (s *server) DeleteRange(ctx context.Context, req *pb.RangeReq) (*pb.DelRangeResp, error) {
	startRowKey := generateRowKey(string(req.Start.BizId), req.Start.Seq) // 生成起始 RowKey
	endRowKey := generateRowKey(string(req.End.BizId), req.End.Seq)       // 生成结束 RowKey

	log.Printf("DeleteRange startRowKey: %s, endRowKey: %s", startRowKey, endRowKey)

	scanRequest, err := hrpc.NewScanRangeStr(ctx, "my_table", startRowKey, endRowKey)
	if err != nil {
		log.Printf("DeleteRange scan request creation failed: %v", err)
		return nil, err // 返回错误
	}
	scanner := s.client.Scan(scanRequest)
	for {
		res, err := scanner.Next()
		if err != nil {
			if err.Error() == "EOF" {
				log.Println("DeleteRange scanner reached end of results")
				break // 扫描结束
			}
			log.Printf("DeleteRange scanner next failed: %v", err)
			return nil, err // 返回错误
		}
		if res == nil {
			log.Println("DeleteRange scanner reached end of results")
			break // 扫描结束
		}
		for _, cell := range res.Cells {
			log.Printf("DeleteRange found cell to delete: %s", cell.Row)
			deleteRequest, err := hrpc.NewDelStr(ctx, "my_table", string(cell.Row), nil)
			if err != nil {
				log.Printf("DeleteRange delete request creation failed: %v", err)
				return nil, err // 返回错误
			}
			_, err = s.client.Delete(deleteRequest)
			if err != nil {
				log.Printf("DeleteRange delete request execution failed: %v", err)
				return nil, err // 返回错误
			}
		}
	}
	log.Println("DeleteRange request successful")
	return &pb.DelRangeResp{}, nil // 返回空的响应
}

// 主函数，启动 gRPC 服务器
func main() {
	lis, err := net.Listen("tcp", ":50052") // 创建一个 TCP 监听器，监听端口 50052
	if err != nil {
		log.Fatalf("failed to listen: %v", err) // 监听失败，记录错误日志并退出
	}

	s := grpc.NewServer()                  // 创建一个新的 gRPC 服务器实例
	pb.RegisterSeqDbServer(s, NewServer()) // 注册 SeqDb 服务到 gRPC 服务器

	fmt.Println("Server is running at :50052") // 打印服务器启动信息
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err) // 服务器启动失败，记录错误日志并退出
	}
}
