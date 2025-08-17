using OsmNightWatch.Lib;
using System.Text.Json;
using Azure.Storage.Blobs;
using Azure.Identity;

internal class IssuesUploader
{
    // Prefer connection string; else use Managed Identity (DefaultAzureCredential) with AZURE_STORAGE_ACCOUNT
    static BlobContainerClient GetContainerClient()
    {
        string containerName = "data";
        var connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION");
        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            return new BlobContainerClient(connectionString, containerName);
        }

        // Simplified: only use AZURE_STORAGE_ACCOUNT
        var accountName = Environment.GetEnvironmentVariable("AZURE_STORAGE_ACCOUNT");
        if (string.IsNullOrWhiteSpace(accountName))
        {
            throw new InvalidOperationException("Azure Storage not configured: set AZURE_STORAGE_CONNECTION or AZURE_STORAGE_ACCOUNT for Managed Identity.");
        }
        var accountUrl = $"https://{accountName}.blob.core.windows.net";

        var credential = new DefaultAzureCredential();
        var serviceClient = new BlobServiceClient(new Uri(accountUrl), credential);
        return serviceClient.GetBlobContainerClient(containerName);
    }

    internal static void Upload(IssuesData data)
    {
        string json = JsonSerializer.Serialize(data);
        var bytes = System.Text.Encoding.UTF8.GetBytes(json);
        using var memStream = new MemoryStream(bytes);

        string fileName = "issues.json";

        BlobContainerClient containerClient = GetContainerClient();
        BlobClient blobClient = containerClient.GetBlobClient(fileName);
        blobClient.Upload(memStream, overwrite: true);
    }

    internal static IssuesData? Download()
    {
        string fileName = "issues.json";

        BlobContainerClient containerClient = GetContainerClient();
        BlobClient blobClient = containerClient.GetBlobClient(fileName);
        if (blobClient.Exists() == false)
            return null;
        var response = blobClient.DownloadContent();

        return JsonSerializer.Deserialize<IssuesData>(response.Value.Content.ToArray());
    }
}