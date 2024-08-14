package main

import (
	"context"
	"fmt"
	"log"

	"demo/gen-go/hbase" // Thrift 生成的 HBase 客户端代码

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
		HOST     = "http://ld-7xv325q01b2720rk9-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:9190"
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

// 创建表
func createTable(client *hbase.THBaseServiceClient, ctx context.Context, tableName string, columnFamily string) error {
	// 创建列族描述符
	columnFamilyDescriptor := &hbase.TColumnFamilyDescriptor{
		Name: []byte(columnFamily),
	}

	// 创建表描述符
	tableDescriptor := &hbase.TTableDescriptor{
		TableName: &hbase.TTableName{
			Ns:        []byte("default"), // 使用默认命名空间
			Qualifier: []byte(tableName),
		},
		Columns: []*hbase.TColumnFamilyDescriptor{columnFamilyDescriptor},
	}

	// 发送创建表请求
	err := client.CreateTable(ctx, tableDescriptor, nil)
	if err != nil {
		// 判断是否为表已存在的错误
		if errMsg := err.Error(); errMsg != "" && (errMsg == "TableExists" || errMsg == "org.apache.hadoop.hbase.TableExistsException") {
			fmt.Printf("Table %s already exists\n", tableName)
			return nil
		}
		return fmt.Errorf("failed to create table: %v", err)
	}

	fmt.Printf("Table %s created successfully\n", tableName)
	return nil
}

func deleteTable(client *hbase.THBaseServiceClient, ctx context.Context, tableName string) error {
	table := &hbase.TTableName{
		Ns:        []byte("default"),
		Qualifier: []byte(tableName),
	}
	err := client.DisableTable(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to disable table: %v", err)
	}
	err = client.DeleteTable(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to delete table: %v", err)
	}
	fmt.Printf("Table %s deleted successfully\n", tableName)
	return nil
}

// 实现 Thrift 服务的 Put 方法
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

// 获取最大 SeqKey
func getMaxKey(client *hbase.THBaseServiceClient, ctx context.Context, tableName string, startPrefix, endPrefix string) ([]byte, error) {
	scan := &hbase.TScan{
		StartRow: []byte(startPrefix),
		StopRow:  []byte(endPrefix),
	}
	scanResults, err := client.GetScannerResults(ctx, []byte(tableName), scan, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to scan for max key: %v", err)
	}
	if len(scanResults) > 0 {
		fmt.Println("GetMaxKey successful")
		return scanResults[0].Row, nil
	}
	return nil, fmt.Errorf("no results found for max key")
}

// 执行范围查询
func queryRange(client *hbase.THBaseServiceClient, ctx context.Context, tableName string, startRowKey, endRowKey string) ([]*hbase.TResult_, error) {
	scan := &hbase.TScan{
		StartRow: []byte(startRowKey),
		StopRow:  []byte(endRowKey),
	}
	results := []*hbase.TResult_{}
	for {
		scanResults, err := client.GetScannerResults(ctx, []byte(tableName), scan, 10)
		if err != nil {
			return nil, fmt.Errorf("failed to query range: %v", err)
		}
		if len(scanResults) == 0 {
			break
		}
		results = append(results, scanResults...)
	}
	fmt.Println("QueryRange successful")
	return results, nil
}

// 删除指定范围的 SeqItems
func deleteRange(client *hbase.THBaseServiceClient, ctx context.Context, tableName string, startRowKey, endRowKey string) error {
	scan := &hbase.TScan{
		StartRow: []byte(startRowKey),
		StopRow:  []byte(endRowKey),
	}
	for {
		scanResults, err := client.GetScannerResults(ctx, []byte(tableName), scan, 10)
		if err != nil {
			return fmt.Errorf("failed to scan for delete range: %v", err)
		}
		if len(scanResults) == 0 {
			break
		}
		for _, result := range scanResults {
			err = deleteItem(client, ctx, tableName, string(result.Row))
			if err != nil {
				return fmt.Errorf("failed to delete item in range: %v", err)
			}
		}
	}
	fmt.Println("DeleteRange successful")
	return nil
}

func main() {
	client, err := createThriftClient()
	if err != nil {
		log.Fatalf("failed to create Thrift client: %v", err)
	}

	ctx := context.Background()

	tableName := "my_table"
	family := "cf"

	// 删除表（如果存在）
	if err := deleteTable(client, ctx, tableName); err != nil {
		fmt.Printf("failed to delete table: %v", err)
	}

	// 创建表
	if err := createTable(client, ctx, tableName, family); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// 插入多条数据
	for i := 1; i <= 5; i++ {
		fileID := fmt.Sprintf("biz%d", i)
		value := fmt.Sprintf("value%d", i)
		rowKey := generateRowKey(fileID, int32(i))
		qualifier := "value"

		if err := putItem(client, ctx, tableName, rowKey, family, qualifier, value); err != nil {
			log.Fatalf("failed to put item: %v", err)
		} else {
			fmt.Printf("Put item for fileID: %s successfully\n", fileID)
		}
	}

	// 获取其中一条数据
	rowKey := generateRowKey("biz1", 1)
	result, err := getItem(client, ctx, tableName, rowKey)
	if err != nil {
		log.Fatalf("failed to get item: %v", err)
	} else {
		fmt.Printf("Get result: %v\n", result)
	}

	// 扫描数据
	results, err := scanItems(client, ctx, tableName, "0", "z")
	if err != nil {
		log.Fatalf("failed to scan items: %v", err)
	}
	for _, res := range results {
		fmt.Printf("Scan result: %v\n", res)
	}

	// 获取最大 Key
	maxKey, err := getMaxKey(client, ctx, tableName, "0", "z")
	if err != nil {
		log.Fatalf("failed to get max key: %v", err)
	}
	fmt.Printf("Max key: %s\n", maxKey)

	// 查询范围内的数据
	rangeResults, err := queryRange(client, ctx, tableName, "0", string(maxKey))
	if err != nil {
		log.Fatalf("failed to query range: %v", err)
	}
	for _, res := range rangeResults {
		fmt.Printf("Range query result: %v\n", res)
	}

	// 删除范围内的数据
	if err := deleteRange(client, ctx, tableName, "0", string(maxKey)); err != nil {
		log.Fatalf("failed to delete range: %v", err)
	}
}
