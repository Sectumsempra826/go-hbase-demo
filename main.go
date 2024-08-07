package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	pb "go-hbase-demo/cloudpb" // 导入生成的 Protobuf 包

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// 计算 file_id 的哈希值:取fileID最后一位
func hash(fileID string) string {
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
	client := gohbase.NewClient("ld-7xv325q01b2720rk9-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:30020")
	return &server{client: client}
}

// 实现 gRPC 服务的 Put 方法
// 将接收到的 SeqItems 存储到 HBase 中
func (s *server) Put(ctx context.Context, seqItems *pb.SeqItems) (*pb.PutItemResp, error) {
	// 插入seqItem
	for _, item := range seqItems.Items {
		rowKey := generateRowKey(string(item.Key.BizId), item.Key.Seq) // 生成 RowKey
		// 序列化 SeqItem
		data, err := proto.Marshal(item)
		if err != nil {
			log.Printf("Failed to marshal SeqItem: %v", err)
		}

		// 创建 HBase Put 请求
		// HBase Shell中建表：create 'my_table','cf'
		putRequest, err := hrpc.NewPutStr(ctx, "my_table", rowKey, map[string]map[string][]byte{
			"cf": { // 列族
				"value": data, // 列名和值
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

// TODO：批量存储
func (s *server) BatchPut(ctx context.Context, seqItemsList []pb.SeqItems) (*pb.PutItemResp, error) {
	//  // 创建一个批次操作的列表
	//  var batch []*hrpc.Mutate

	//  // 遍历每个 SeqItems
	//  for i := range seqItemsList {
	// 	 seqItems := &seqItemsList[i]
	// 	 // 遍历每个 SeqItem
	// 	 for j := range seqItems.Items {
	// 		 item := &seqItems.Items[j]
	// 		 rowKey := generateRowKey(string(item.Key.BizId), item.Key.Seq) // 生成 RowKey
	// 		 // 序列化 SeqItem
	// 		 data, err := proto.Marshal(item)
	// 		 if err != nil {
	// 			 log.Printf("Failed to marshal SeqItem: %v", err)
	// 			 return nil, err // 返回错误
	// 		 }

	// 		 // 创建 HBase Put 请求
	// 		 putRequest, err := hrpc.NewPutStr(ctx, "my_table", rowKey, map[string]map[string][]byte{
	// 			 "cf": { // 列族
	// 				 "value": data, // 列名和值
	// 			 },
	// 		 })
	// 		 if err != nil {
	// 			 log.Printf("Put request creation failed: %v", err)
	// 			 return nil, err // 返回错误
	// 		 }

	// 		 // 将每个 putRequest 加入到批次列表中
	// 		 batch = append(batch, putRequest)
	// 	 }
	//  }

	// // 执行批次操作
	// for _, put := range batch {
	//     _, err := s.client.Put(put)
	//     if err != nil {
	//         log.Printf("Batch put request execution failed: %v", err)
	//         return nil, err // 返回错误
	//     }
	// }

	// log.Println("BatchPut request successful")
	return &pb.PutItemResp{}, nil // 返回空的响应
}

// 实现 gRPC 服务的 Get 方法
// 根据 SeqKey 从 HBase 中检索数据
func (s *server) Get(ctx context.Context, seqKey *pb.SeqKey) (*pb.SeqItem, error) {
	rowKey := generateRowKey(string(seqKey.BizId), seqKey.Seq) // 生成 RowKey
	// 创建 HBase Get 请求,根据rowkey查找seqitem
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

	seqItem := &pb.SeqItem{}
	// 反序列化为seqitem
	err = proto.Unmarshal(value, seqItem)
	if err != nil {
		log.Printf("Failed to unmarshal SeqItem: %v", err)
		return nil, err // 返回错误
	}
	log.Println("Get request successful： " + seqItem.String())
	return seqItem, nil // 返回 SeqItem
}

// TODO 测试
// 批量获取
// 实现 gRPC 服务的 BatchGet 方法
// 根据多个 SeqKey 从 HBase 中检索数据
func (s *server) BatchGet(ctx context.Context, seqKeys []*pb.SeqKey) (*pb.SeqItems, error) {
	// 创建一个批次操作的列表
	var batch []*hrpc.Get

	// 遍历每个 SeqKey
	for _, seqKey := range seqKeys {
		rowKey := generateRowKey(string(seqKey.BizId), seqKey.Seq) // 生成 RowKey
		// 创建 HBase Get 请求
		getRequest, err := hrpc.NewGetStr(ctx, "my_table", rowKey)
		if err != nil {
			log.Printf("Get request creation failed: %v", err)
			return nil, err // 返回错误
		}
		// 将每个 getRequest 加入到批次列表中
		batch = append(batch, getRequest)
	}

	// 创建一个用于存储结果的切片
	var seqItems []*pb.SeqItem

	// 执行批次操作
	for _, getRequest := range batch {
		getRsp, err := s.client.Get(getRequest)
		if err != nil {
			log.Printf("Get request execution failed: %v", err)
			return nil, err // 返回错误
		}
		if len(getRsp.Cells) == 0 {
			log.Printf("No data found for row key: %s", getRequest.Key())
			continue
		}
		value := getRsp.Cells[0].Value // 获取返回值

		seqItem := &pb.SeqItem{}
		// 反序列化为 seqItem
		err = proto.Unmarshal(value, seqItem)
		if err != nil {
			log.Printf("Failed to unmarshal SeqItem: %v", err)
			return nil, err // 返回错误
		}
		seqItems = append(seqItems, seqItem)
	}

	log.Println("BatchGet request successful")
	return &pb.SeqItems{Items: seqItems}, nil // 返回 SeqItems
}

// 实现 gRPC 服务的 GetMaxKey 方法
// 获取最大 SeqKey
func (s *server) GetMaxKey(ctx context.Context, seqKey *pb.SeqKey) (*pb.SeqKey, error) {
	// 构造范围扫描的开始和结束前缀
	startPrefix := generateRowKey(string(seqKey.BizId), ^int32(0))
	endPrefix := generateRowKey(string(seqKey.BizId), int32(0))

	// 执行范围扫描查询
	scanRequest, err := hrpc.NewScanRange(ctx, []byte("my_table"), []byte(startPrefix), []byte(endPrefix))
	if err != nil {
		log.Printf("GetMaxKey request creation failed: %v", err)
		return nil, err
	}
	scanner := s.client.Scan(scanRequest)

	var maxSeqKey *pb.SeqKey
	// 迭代扫描结果
	for {
		res, err := scanner.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("GetMaxKey scan result error: %v", err)
			return nil, err
		}
		for _, cell := range res.Cells {
			// 反序列化 cell.Value 成 SeqItem
			seqItem := &pb.SeqItem{}
			err = proto.Unmarshal(cell.Value, seqItem)
			if err != nil {
				log.Printf("Failed to unmarshal SeqItem: %v", err)
				continue
			}

			// 从 SeqItem 中获取 SeqKey
			seqKey := seqItem.GetKey()
			if seqKey == nil {
				log.Printf("SeqItem does not contain SeqKey")
				continue
			}

			// 比较 SeqKey 的 seq 值
			if maxSeqKey == nil || seqKey.GetSeq() > maxSeqKey.GetSeq() {
				maxSeqKey = seqKey
			}
		}
	}

	if maxSeqKey == nil {
		log.Println("GetMaxKey request found no matching cells")
		return nil, fmt.Errorf("no matching cells found")
	}

	log.Println("GetMaxKey request successful: " + maxSeqKey.String())
	return maxSeqKey, nil
}

// 辅助方法：根据RangeOption处理区间，生成边界rowkey
func generateQueryRangeKeys(req *pb.RangeReq) (startRowKey string, endRowKey string) {
	// 由于NewScanRangeStr方法默认为左闭右开，左开和右闭的时候需要处理
	// 左开：withoutboth withoustart 右闭：withoutstart withboth

	// 顺序时startRowKey<endRowKey, 倒序时startRowKey>endRowKey

	if req.Reverse == false { // 顺序
		if req.Option == pb.RangeOption_WithoutStart || req.Option == pb.RangeOption_WithoutBoth {
			// 左开区间，startRowkey+1
			startRowKey = generateRowKey(string(req.Start.BizId), req.Start.Seq-1)
			endRowKey = generateRowKey(string(req.End.BizId), req.End.Seq)
		}
		if req.Option == pb.RangeOption_WithoutStart || req.Option == pb.RangeOption_WithBoth {
			// 右闭区间，endRowkey+1
			startRowKey = generateRowKey(string(req.Start.BizId), req.Start.Seq)
			endRowKey = generateRowKey(string(req.End.BizId), req.End.Seq-1)
		}
		if req.Option == pb.RangeOption_WithoutEnd {
			// 左闭右开，无需处理
			startRowKey = generateRowKey(string(req.Start.BizId), req.Start.Seq)
			endRowKey = generateRowKey(string(req.End.BizId), req.End.Seq)
		}
	} else if req.Reverse == true { // 倒序
		if req.Option == pb.RangeOption_WithoutStart || req.Option == pb.RangeOption_WithoutBoth {
			// 左开，startRowkey-1
			endRowKey = generateRowKey(string(req.Start.BizId), req.Start.Seq)
			startRowKey = generateRowKey(string(req.End.BizId), req.End.Seq+1)
		}
		if req.Option == pb.RangeOption_WithoutStart || req.Option == pb.RangeOption_WithBoth {
			// 右闭，endRowkey-1
			endRowKey = generateRowKey(string(req.Start.BizId), req.Start.Seq+1)
			startRowKey = generateRowKey(string(req.End.BizId), req.End.Seq)
		}
		if req.Option == pb.RangeOption_WithoutEnd {
			// 左闭右开，无需处理
			endRowKey = generateRowKey(string(req.Start.BizId), req.Start.Seq)
			startRowKey = generateRowKey(string(req.End.BizId), req.End.Seq)
		}
	}

	// TODO
	// startKey := []byte("startKey")
	// endKey := []byte("endKey")

	// startKeyWithZero := append(startKey, "\\0")
	// endKeyWithZero := append(endKey, "\\0")

	// if req.Option == pb.RangeOption_WithoutBoth{
	// 	startRowKey = startKeyWithZero;
	// 	endRowKey = endKey;
	// }

	return startRowKey, endRowKey
}

// 实现 gRPC 服务的 QueryRange 方法
// 根据范围请求检索 SeqItems
func (s *server) QueryRange(ctx context.Context, req *pb.RangeReq) (*pb.SeqItems, error) {
	// 根据RangeOption生成边界rowkey
	startRowKey, endRowKey := generateQueryRangeKeys(req)

	log.Printf("QueryRange startRowKey: %s, endRowKey: %s", startRowKey, endRowKey)

	// 创建扫描请求
	var scanRequest *hrpc.Scan
	var err error

	if req.Reverse {
		scanRequest, err = hrpc.NewScanRangeStr(ctx, "my_table", startRowKey, endRowKey, hrpc.Reversed(), hrpc.NumberOfRows(10))
	} else {
		scanRequest, err = hrpc.NewScanRangeStr(ctx, "my_table", startRowKey, endRowKey)
	}

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
	// 打印每个 SeqItem
	for _, item := range items {
		log.Printf("SeqItem: Key=%s, Value=%s", item.Key.BizId, string(item.Value))
	}
	return &pb.SeqItems{Items: items}, nil // 返回 SeqItems
}

// 实现 gRPC 服务的 DeleteRange 方法
// 删除指定范围的 SeqItems
func (s *server) DeleteRange(ctx context.Context, req *pb.RangeReq) (*pb.DelRangeResp, error) {
	// 根据 RangeOption 处理区间
	startRowKey, endRowKey := generateQueryRangeKeys(req)
	log.Printf("DeleteRange startRowKey: %s, endRowKey: %s", startRowKey, endRowKey)

	// 创建扫描请求
	var scanRequest *hrpc.Scan
	var err error

	if req.Reverse {
		// For reverse scanning, swap start and end keys and process results in reverse
		scanRequest, err = hrpc.NewScanRangeStr(ctx, "my_table", startRowKey, endRowKey, hrpc.Reversed())
	} else {
		scanRequest, err = hrpc.NewScanRangeStr(ctx, "my_table", startRowKey, endRowKey)
	}

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
	lis, err := net.Listen("tcp", ":30020") // 创建一个 TCP 监听器，监听端口30020
	if err != nil {
		log.Fatalf("failed to listen: %v", err) // 监听失败，记录错误日志并退出
	}

	s := grpc.NewServer()                  // 创建一个新的 gRPC 服务器实例
	pb.RegisterSeqDbServer(s, NewServer()) // 注册 SeqDb 服务到 gRPC 服务器

	fmt.Println("Server is running at :30020") // 打印服务器启动信息
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err) // 服务器启动失败，记录错误日志并退出
	}
}
