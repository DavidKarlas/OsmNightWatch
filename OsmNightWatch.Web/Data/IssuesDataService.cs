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
            cache = client.GetFromJsonAsync<IssuesData>("https://davidupload.blob.core.windows.net/data/issues.json")!;
            lastCache.Restart();
            return cache;
        }

        Task<RateLimitedAccount[]>? cacheSuspicious = null;
        Stopwatch lastSuspiciousCache = Stopwatch.StartNew();

        public Task<RateLimitedAccount[]> GetSuspiciousDataAsync()
        {
            if (cacheSuspicious != null && lastSuspiciousCache.Elapsed.TotalMinutes < 1)
            {
                return cacheSuspicious;
            }
            cacheSuspicious = client.GetFromJsonAsync<RateLimitedAccount[]>("https://davidupload.blob.core.windows.net/data/RateLimit.json")!;
            lastCache.Restart();
            return cacheSuspicious;
        }
    }

    public class RateLimitedAccount
    {
        public long UserId { get; set; }
        public string Username { get; set; }
        public bool Blocked { get; set; }
        public bool Deleted { get; set; }
        public DateTime CreatedDate { get; set; }
        public int TotalChangesets { get; set; }
        public DateTime FirstChangesetDate { get; set; }
        public List<RateLimitedChangeset> Changesets { get; set; }

        public class RateLimitedChangeset
        {
            public long Id { get; set; }
            public DateTime Timestamp { get; set; }
            public List<string> Reasons { get; set; }
        }
    }

}