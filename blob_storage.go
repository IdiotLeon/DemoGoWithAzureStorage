package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
)

// https://github.com/Azure-Samples/storage-blobs-go-quickstart/blob/master/storage-quickstart.go

// Azure Storage Quickstart Sample - Demonstrate how to upload, list, download, and delete blobs.
//
// Documentation References:
// - What is a Storage Account - https://docs.microsoft.com/azure/storage/common/storage-create-storage-account
// - Blob Service Concepts - https://docs.microsoft.com/rest/api/storageservices/Blob-Service-Concepts
// - Blob Service Go SDK API - https://godoc.org/github.com/Azure/azure-storage-blob-go
// - Blob Service REST API - https://docs.microsoft.com/rest/api/storageservices/Blob-Service-REST-API
// - Scalability and performance targets - https://docs.microsoft.com/azure/storage/common/storage-scalability-targets
// - Azure Storage Performance and Scalability checklist https://docs.microsoft.com/azure/storage/common/storage-performance-checklist
// - Storage Emulator - https://docs.microsoft.com/azure/storage/common/storage-use-emulator

func randomString() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return strconv.Itoa(r.Int())
}

func handleErrors(err error) {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare seviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				fmt.Println("Received 409. Container already exists")
				return
			}
		}

		log.Fatal(err)
	}
}

func main() {
	fmt.Printf("Azure blob storage quick start sample\n")

	storageAccountName := ""
	storageAccountKey := ""

	// To create a default request pipeline using one's storage account name and account key
	credential := azblob.NewSharedKeyCredential(storageAccountName, storageAccountKey)
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// To create a random string for the quick start container
	containerName := fmt.Sprintf("quickstart-%s", randomString())
	fmt.Println("containerName: " + containerName)

	// From the Azure portal, to get your storage account blob service URL endpoint
	URL, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", storageAccountName, containerName))
	if err != nil {
		fmt.Println(err)
		return
	}

	// To create a ContainerURL object that wraps the container URL and a request pipeline to make requests
	containerURL := azblob.NewContainerURL(*URL, p)

	// To create the container
	fmt.Printf("Creating a container named %s\n", containerName)
	ctx := context.Background() // This example uses a never-expiring context
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	handleErrors(err)

	// To create a file to test the upload and download
	fmt.Printf("Creating a dummy file to test the upload and download\n")
	data := []byte("Hello World\nthis is a blob\n")
	fileName := randomString()
	err = ioutil.WriteFile(fileName, data, 0700)
	handleErrors(err)

	// To upload a blob
	blobURL := containerURL.NewBlockBlobURL(fileName)
	file, err := os.Open(fileName)
	handleErrors(err)

	// You can use the low-level PutBlob API to upload files. Low-level APIs are simple wrappers for the Azure Storage REST APIs.
	// Note that PutBlob can upload up to 256MB data in one shot. Details: https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob
	// Following is commented out intentionally because we will instead use UploadFileToBlockBlob API to upload the blob
	// _, err = blobURL.PutBlob(ctx, file, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	// handleErrors(err)

	// The high-level API UploadFileToBlockBlob function uploads blocks in parallel for optimal performance, and can handle large files as well.
	// This function calls PutBlock/PutBlockList for files larger 256 MBs, and calls PutBlob for any file smaller
	fmt.Printf("Uploading the file with blob name: %s\n", fileName)
	_, err = azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})
	handleErrors(err)

	// To list the blobs in the container
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// To get a result segment starting with the blob indicated by the current marker
		listBlob, err := containerURL.ListBlobs(ctx, marker, azblob.ListBlobsOptions{})
		handleErrors(err)

		// ListBlobs return the start of the next segment; one MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// To process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Blobs.Blob {
			fmt.Print("Blob name: " + blobInfo.Name + "\n")
		}
	}

	// To download the blob
	// This method automatically retries if the connection fails during download
	// (the low-level GetBlob function does NOT retry errors when reading from its stream)
	stream := azblob.NewDownloadStream(ctx, blobURL.GetBlob, azblob.DownloadStreamOptions{})
	downloadedData := &bytes.Buffer{}
	_, err = downloadedData.ReadFrom(stream)
	handleErrors(err)

	// To print the downloaded blob data in downloadData's buffer
	fmt.Printf("Downloaded the blob: " + downloadedData.String())

	// To clean up the quick start by deleting the container and the file created locally
	fmt.Printf("Press enter key to delete the sample files, example container, and exit the application. \n")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
	fmt.Printf("Cleaning up.\n")
	containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	file.Close()
	os.Remove(fileName)
}
