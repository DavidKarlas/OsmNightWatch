using OsmNightWatch.Lib;

namespace OsmNightWatch.Web.Data
{
    //TODO: Cache for 1 minute...
    public class IssuesDataService
    {
        HttpClient client = new HttpClient();
        
        public Task<IssuesData?> GetIssuesDataAsync(DateTime startDate)
        {
            return client.GetFromJsonAsync<IssuesData>("https://davidupload.blob.core.windows.net/data/issues.json");
        }
    }
}