using OsmNightWatch.Lib;
using System.Text.Json;
using Azure.Storage.Blobs;

internal class IssuesUploader
{
    static readonly string blobStorageConnectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION");

    internal static async Task UploadAsync(IssuesData data)
    {
        string json = JsonSerializer.Serialize(data);
        var bytes = System.Text.Encoding.UTF8.GetBytes(json);
        var memStream = new MemoryStream(bytes);

        string blobStorageContainerName = "data";
        string fileName = "issues.json";

        BlobContainerClient containerClient = new(blobStorageConnectionString, blobStorageContainerName);
        BlobClient blobClient = containerClient.GetBlobClient(fileName);
        await blobClient.UploadAsync(memStream, overwrite: true);
    }
}