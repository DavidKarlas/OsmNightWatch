using OsmNightWatch.Lib;
using System.Text.Json;
using Azure.Storage.Blobs;

internal class IssuesUploader
{
    static readonly string blobStorageConnectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION");

    internal static void Upload(IssuesData data)
    {
        string json = JsonSerializer.Serialize(data);
        var bytes = System.Text.Encoding.UTF8.GetBytes(json);
        var memStream = new MemoryStream(bytes);

        string blobStorageContainerName = "data";
        string fileName = "issues2.json";

        BlobContainerClient containerClient = new(blobStorageConnectionString, blobStorageContainerName);
        BlobClient blobClient = containerClient.GetBlobClient(fileName);
        blobClient.Upload(memStream, overwrite: true);
    }

    internal static IssuesData? Download()
    {
        string blobStorageContainerName = "data";
        string fileName = "issues2.json";

        BlobContainerClient containerClient = new(blobStorageConnectionString, blobStorageContainerName);
        BlobClient blobClient = containerClient.GetBlobClient(fileName);
        var response = blobClient.DownloadContent();

        return JsonSerializer.Deserialize<IssuesData>(response.Value.Content.ToArray());
    }
}