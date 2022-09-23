using OsmNightWatch.Lib;
using System.Text.Json;
using Azure.Storage.Blobs;

internal class IssuesUploader
{
    internal static void Upload(IssuesData data)
    {
        string json = JsonSerializer.Serialize(data);
        var bytes = System.Text.Encoding.UTF8.GetBytes(json);
        var memStream = new MemoryStream(bytes);

        string blobStorageConnectionString = "";
        string blobStorageContainerName = "data";
        string fileName = "issues.json";

        BlobContainerClient containerClient = new(blobStorageConnectionString, blobStorageContainerName);
        BlobClient blobClient = containerClient.GetBlobClient(fileName);
        blobClient.UploadAsync(memStream, overwrite: true);
    }
}