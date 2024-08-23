using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using OsmSharp.Logging;

namespace OsmSharp.Replication.Http
{
    internal class HttpHandler : IHttpHandler
    {
        internal static readonly ThreadLocal<HttpClient> ThreadLocalClient =
            new ThreadLocal<HttpClient>(() => new HttpClient());
        internal static readonly Lazy<IHttpHandler> LazyHttpHandler = 
            new Lazy<IHttpHandler>(() => new HttpHandler());
        
        public async Task<Stream?> TryGetStreamAsync(string requestUri)
        {
            var client = ThreadLocalClient.Value;
            try
            {
                var response = await client.GetAsync(requestUri);
                if (response.IsSuccessStatusCode) return await response.Content.ReadAsStreamAsync();

                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    return null;
                }
                
                throw new HttpRequestException("Unexpected response.");
            }
            catch (Exception e)
            {
                Logger.Log($"{nameof(HttpHandler)}.{nameof(TryGetStreamAsync)}", TraceEventType.Error,
                    $"Unhandled exception when getting {requestUri}: {e}");
                throw;
            }
        }

        private static IHttpHandler? _defaultHandler;

        /// <summary>
        /// Gets or sets the default http handler.
        /// </summary>
        public static IHttpHandler Default
        {
            get => _defaultHandler ??= LazyHttpHandler.Value;
            set => _defaultHandler = value;
        }
    }
}