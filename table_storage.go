package main

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/storage"
)

const (
	TABLE_NAME                 = ""
	STORAGE_ACCOUNT_NAME       = ""
	STORAGE_ACCOUNT_ACCESS_KEY = ""
)

func main() {
	client, err := newClient(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_ACCESS_KEY)
	if err != nil {
		fmt.Println(err)
	}

	tableService := client.GetTableService()
	table := tableService.GetTableReference(TABLE_NAME)

	entity := table.GetEntityReference("PartitionKey", "RowKey")

	err = entity.Get(30, storage.FullMetadata, &storage.GetEntityOptions{
		Select: []string{
			"one property",
		},
	})
	if err != nil {
		fmt.Println(err)
	}

	result := entity.Properties["property"].(string)
	fmt.Println(result)
}

func newClient(name, key string) (*storage.Client, error) {
	client, err := storage.NewBasicClient(name, key)
	if err != nil {
		return nil, err
	}
	return &client, err
}
