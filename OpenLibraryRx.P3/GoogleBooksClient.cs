using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Newtonsoft.Json.Linq;

namespace OpenLibraryRx.P3
{
    public sealed class GoogleBooksClient : IDisposable
    {
        private readonly HttpClient _http;

        public GoogleBooksClient()
        {
            ServicePointManager.SecurityProtocol |= SecurityProtocolType.Tls12;

            var handler = new HttpClientHandler
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
            };

            _http = new HttpClient(handler, disposeHandler: true)
            {
                Timeout = TimeSpan.FromSeconds(15)
            };

            _http.DefaultRequestHeaders.Accept.ParseAdd("application/json");
            _http.DefaultRequestHeaders.UserAgent.ParseAdd("GoogleBooksRx-P3/1.0");
        }

        public async Task<(int totalItems, List<Volume> items)> SearchByAuthorAsync(
            string author, int startIndex, int maxResults, CancellationToken ct)
        {
            if (maxResults < 1) maxResults = 20;
            if (maxResults > 40) maxResults = 40;
            if (startIndex < 0) startIndex = 0;

            var q = "inauthor:" + author;
            var url = new StringBuilder("https://www.googleapis.com/books/v1/volumes?");
            url.Append("q=").Append(HttpUtility.UrlEncode(q));
            url.Append("&startIndex=").Append(startIndex);
            url.Append("&maxResults=").Append(maxResults);
            url.Append("&printType=books");
            
            url.Append("&fields=totalItems,items(volumeInfo/title,volumeInfo/description,volumeInfo/infoLink)");

            using (var resp = await _http.GetAsync(url.ToString(), HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false))
            {
                resp.EnsureSuccessStatusCode();
                var json = await resp.Content.ReadAsStringAsync().ConfigureAwait(false);
                var root = JObject.Parse(json);

                int total = root.Value<int?>("totalItems") ?? 0;

                var list = new List<Volume>();
                var items = root["items"] as JArray;
                if (items != null)
                {
                    foreach (var it in items)
                    {
                        var vi = it["volumeInfo"] as JObject;
                        if (vi == null) continue;

                        var title = vi.Value<string>("title");
                        var desc = vi.Value<string>("description") ?? "";
                        var link = vi.Value<string>("infoLink") ?? "#";

                        list.Add(new Volume
                        {
                            Title = title ?? "",
                            Description = desc,
                            InfoLink = link
                        });
                    }
                }

                return (total, list);
            }
        }

        public void Dispose() => _http.Dispose();

        public sealed class Volume
        {
            public string Title { get; set; }
            public string Description { get; set; }
            public string InfoLink { get; set; }
        }
    }
}
