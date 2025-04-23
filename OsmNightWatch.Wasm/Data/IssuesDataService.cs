using OsmNightWatch.Lib;
using System.Diagnostics;
using System.Net.Http.Json;

namespace OsmNightWatch.Wasm.Data
{
    public class IssuesDataService
    {
        HttpClient client = new HttpClient();
        Task<IssuesData>? cache = null;
        Stopwatch lastCache = Stopwatch.StartNew();

        public IssuesDataService()
        {
            KeepUpdatedForever();
        }

        private async void KeepUpdatedForever()
        {
            long lastMinute = -1;
            while (true)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(1));
                    var fetch = await GetIssuesDataAsync();
                    if (lastMinute != fetch.MinutelySequenceNumber)
                    {
                        lastMinute = fetch.MinutelySequenceNumber;
                        IssuesDataChanged?.Invoke(fetch);
                    }
                }
                catch
                {
                }
            }
        }

        public event Action<IssuesData>? IssuesDataChanged;

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
    }
}