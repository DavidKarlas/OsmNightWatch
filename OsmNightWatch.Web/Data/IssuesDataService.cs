using OsmNightWatch.Lib;
using System.Diagnostics;

namespace OsmNightWatch.Web.Data
{
    //TODO: Cache for 1 minute...
    public class IssuesDataService
    {
        HttpClient client = new HttpClient();
        Task<IssuesData>? cache = null;
        Stopwatch lastCache = Stopwatch.StartNew();

        public Task<IssuesData> GetIssuesDataAsync()
        {
            if (cache != null && lastCache.Elapsed.TotalMinutes < 1)
            {
                return cache;
            }
            cache = client.GetFromJsonAsync<IssuesData>("https://davidupload.blob.core.windows.net/data/issues2.json")!;
            lastCache.Restart();
            return cache;
        }
    }
}