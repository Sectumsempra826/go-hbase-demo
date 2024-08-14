package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"./go/demo/gen-go/hbase" // Thrift 生成的 HBase 客户端代码
	"github.com/apache/thrift/lib/go/thrift"
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

// 创建 Thrift 客户端并连接到 HBase
func createThriftClient() (*hbase.THBaseServiceClient, error) {
	const (
		HOST     = "ld-7xv325q01b2720rk9-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:9190"
		USER     = "root"
		PASSWORD = "GkWWCUPb"
	)

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	trans, err := thrift.NewTHttpClient(HOST)
	if err != nil {
		return nil, fmt.Errorf("error resolving address: %v", err)
	}

	// 设置用户名和密码
	httpClient := trans.(*thrift.THttpClient)
	httpClient.SetHeader("ACCESSKEYID", USER)
	httpClient.SetHeader("ACCESSSIGNATURE", PASSWORD)

	client := hbase.NewTHBaseServiceClientFactory(trans, protocolFactory)
	if err := trans.Open(); err != nil {
		return nil, fmt.Errorf("Error opening %s: %v", HOST, err)
	}
	return client, nil
}

// 实现 Thrift 服务的 Put 方法
// 将接收到的 SeqItems 存储到 HBase 中
func putItem(client *hbase.THBaseServiceClient, ctx context.Context, tableName string, rowKey string, family string, qualifier string, value string) error {
	put := &hbase.TPut{
		Row: []byte(rowKey),
		ColumnValues: []*hbase.TColumnValue{
			{
				Family:    []byte(family),
				Qualifier: []byte(qualifier),
				Value:     []byte(value),
			},
		},
	}
	err := client.Put(ctx, []byte(tableName), put)
	if err != nil {
		return fmt.Errorf("failed to put item: %v", err)
	}
	fmt.Println("Put successful")
	return nil
}

// 实现 Thrift 服务的 Get 方法
// 根据 SeqKey 从 HBase 中检索数据
func getItem(client *hbase.THBaseServiceClient, ctx context.Context, tableName string, rowKey string) (*hbase.TResult_, error) {
	get := &hbase.TGet{
		Row: []byte(rowKey),
	}
	result, err := client.Get(ctx, []byte(tableName), get)
	if err != nil {
		return nil, fmt.Errorf("failed to get item: %v", err)
	}
	fmt.Println("Get successful")
	return result, nil
}

// 实现 Thrift 服务的 Delete 方法
// 根据 SeqKey 删除 HBase 中的数据
func deleteItem(client *hbase.THBaseServiceClient, ctx context.Context, tableName string, rowKey string) error {
	delete := &hbase.TDelete{
		Row: []byte(rowKey),
	}
	err := client.DeleteSingle(ctx, []byte(tableName), delete)
	if err != nil {
		return fmt.Errorf("failed to delete item: %v", err)
	}
	fmt.Println("Delete successful")
	return nil
}

// 实现 Thrift 服务的 Scan 方法
// 执行范围扫描
func scanItems(client *hbase.THBaseServiceClient, ctx context.Context, tableName string, startRowKey string, stopRowKey string) ([]*hbase.TResult_, error) {
	scan := &hbase.TScan{
		StartRow: []byte(startRowKey),
		StopRow:  []byte(stopRowKey),
	}
	results := []*hbase.TResult_{}
	for {
		result, err := client.GetScannerResults(ctx, []byte(tableName), scan, 10)
		if err != nil {
			return nil, fmt.Errorf("failed to scan items: %v", err)
		}
		if len(result) == 0 {
			break
		}
		results = append(results, result...)
	}
	fmt.Println("Scan successful")
	return results, nil
}

func main() {
	client, err := createThriftClient()
	if err != nil {
		log.Fatalf("failed to create Thrift client: %v", err)
	}

	ctx := context.Background()

	tableName := "my_table"
	rowKey := generateRowKey("example_file_id", 1)
	family := "cf"
	qualifier := "value"
	value := "example_value"

	// 插入数据
	if err := putItem(client, ctx, tableName, rowKey, family, qualifier, value); err != nil {
		log.Fatalf("failed to put item: %v", err)
	}

	// 获取数据
	result, err := getItem(client, ctx, tableName, rowKey)
	if err != nil {
		log.Fatalf("failed to get item: %v", err)
	}
	fmt.Printf("Get result: %v\n", result)

	// 扫描数据
	results, err := scanItems(client, ctx, tableName, "0", "z")
	if err != nil {
		log.Fatalf("failed to scan items: %v", err)
	}
	for _, res := range results {
		fmt.Printf("Scan result: %v\n", res)
	}

	// 删除数据
	if err := deleteItem(client, ctx, tableName, rowKey); err != nil {
		log.Fatalf("failed to delete item: %v", err)
	}
}

