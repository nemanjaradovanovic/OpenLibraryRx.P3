using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Newtonsoft.Json;

namespace OpenLibraryRx.P3
{
    public sealed class WebServer : IDisposable
    {
        private readonly HttpListener _listener = new HttpListener();
        private Thread _acceptThread;
        private volatile bool _running;

        private readonly Subject<HttpListenerContext> _incoming = new Subject<HttpListenerContext>();
        private IDisposable _subscription;

        private readonly object _consoleLock = new object();

        private readonly GoogleBooksClient _gb = new GoogleBooksClient();

        private readonly ResponseCache _cache = new ResponseCache(TimeSpan.FromMinutes(5));

        public WebServer(string prefix)
        {
            _listener.Prefixes.Add(prefix);
        }

        public void Start()
        {
            _subscription = _incoming
                .ObserveOn(TaskPoolScheduler.Default)
                .SelectMany(ctx => Observable.FromAsync(() => HandleRequestAsync(ctx)))
                .Subscribe(
                    _ => { },
                    ex => SafeLog($"PIPELINE ERROR: {ex}")
                );

            _listener.Start();
            _running = true;

            _acceptThread = new Thread(AcceptLoop)
            {
                IsBackground = true,
                Name = "Http-Acceptor"
            };
            _acceptThread.Start();
        }

        private void AcceptLoop()
        {
            while (_running)
            {
                HttpListenerContext ctx = null;
                try
                {
                    ctx = _listener.GetContext(); 
                }
                catch (ObjectDisposedException) { break; }
                catch (HttpListenerException) { break; }
                catch (Exception ex) { SafeLog($"ACCEPT ERROR: {ex}"); }

                if (ctx == null) continue;

                _incoming.OnNext(ctx);
            }
        }

        private async Task HandleRequestAsync(HttpListenerContext ctx)
        {
            var req = ctx.Request;
            var res = ctx.Response;
            var sw = Stopwatch.StartNew();
            int status = 200;
            var thr = Thread.CurrentThread.ManagedThreadId;

            try
            {
                if (!string.Equals(req.HttpMethod, "GET", StringComparison.OrdinalIgnoreCase))
                {
                    status = 405;
                    await WriteTextAsync(ctx, status, "Only GET is allowed.", "text/plain");
                    return;
                }

                var path = req.Url?.AbsolutePath?.TrimEnd('/').ToLowerInvariant() ?? "/";

                if (path == "" || path == "/")
                {
                    await WriteHtmlAsync(ctx, BuildLandingHtml());
                    return;
                }

                if (path == "/health")
                {
                    await WriteJsonAsync(ctx, new { status = "ok", cache = _cache.Stats, time = DateTime.UtcNow });
                    return;
                }

                if (path == "/books")
                {
                    var qs = HttpUtility.ParseQueryString(req.Url.Query);
                    var author = (qs["author"] ?? "").Trim();
                    bool wantHtml = string.Equals(qs["format"], "html", StringComparison.OrdinalIgnoreCase);

                    int page = 1;
                    int.TryParse(qs["page"], out page);
                    if (page < 1) page = 1;

                    int limit = 20;
                    int.TryParse(qs["limit"], out limit);
                    if (limit < 1) limit = 20;
                    if (limit > 40) limit = 40; // Google Books max

                    if (string.IsNullOrWhiteSpace(author))
                    {
                        status = 400;
                        var msg = "Provide ?author=... to search.";
                        if (wantHtml)
                            await WriteHtmlAsync(ctx, BuildErrorHtml(msg));
                        else
                            await WriteJsonAsync(ctx, new { error = msg, status });
                        return;
                    }

                    var cacheKey = BuildCacheKey(author, page, limit, wantHtml);
                    if (_cache.TryGet(cacheKey, out var cached))
                    {
                        await WriteBytesAsync(ctx, 200, cached, wantHtml ? "text/html; charset=utf-8" : "application/json; charset=utf-8");
                        return;
                    }

                    int startIndex = (page - 1) * limit;

                    using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
                    {
                        var (totalItems, volumes) = await _gb.SearchByAuthorAsync(author, startIndex, limit, cts.Token).ConfigureAwait(false);

                        if (totalItems <= 0 || volumes.Count == 0)
                        {
                            status = 404;
                            var msg = "No books found for given author.";
                            if (wantHtml)
                                await WriteHtmlAsync(ctx, BuildErrorHtml(msg));
                            else
                                await WriteJsonAsync(ctx, new { error = msg, status });
                            return;
                        }

                        var processed = await volumes
                            .ToObservable()
                            .ObserveOn(TaskPoolScheduler.Default)
                            .Select(v =>
                            {
                                var (upper, uniq) = AnalyzeDescription(v.Description);
                                return new BookView
                                {
                                    Title = v.Title,
                                    Description = v.Description,
                                    InfoLink = v.InfoLink,
                                    UppercaseCount = upper,
                                    UniqueCount = uniq
                                };
                            })
                            .ToList()
                            .ToTask()
                            .ConfigureAwait(false);

                        processed = processed
                            .OrderByDescending(x => x.UppercaseCount)
                            .ThenByDescending(x => x.UniqueCount)
                            .ToList();

                        if (wantHtml)
                        {
                            var html = BuildBooksHtml(author, page, limit, totalItems, processed);
                            var payload = Encoding.UTF8.GetBytes(html);
                            _cache.Set(cacheKey, payload); 
                            await WriteBytesAsync(ctx, 200, payload, "text/html; charset=utf-8");
                        }
                        else
                        {
                            var response = new
                            {
                                ok = true,
                                query = new { author, page, limit },
                                total = totalItems,
                                count = processed.Count,
                                items = processed.Select(x => new
                                {
                                    title = x.Title,
                                    description = x.Description,
                                    uppercaseWords = x.UppercaseCount,
                                    uniqueWords = x.UniqueCount,
                                    infoLink = x.InfoLink
                                }).ToList()
                            };
                            var json = JsonConvert.SerializeObject(response, Formatting.Indented);
                            var payload = Encoding.UTF8.GetBytes(json);
                            _cache.Set(cacheKey, payload); 
                            await WriteBytesAsync(ctx, 200, payload, "application/json; charset=utf-8");
                        }
                        return;
                    }
                }

                status = 404;
                await WriteTextAsync(ctx, status, "Not Found", "text/plain");
            }
            catch (Exception ex)
            {
                status = 500;
                await WriteJsonAsync(ctx, new { error = "Internal server error.", detail = ex.Message, status });
            }
            finally
            {
                sw.Stop();
                var ms = (int)sw.ElapsedMilliseconds;
                var ip = req.RemoteEndPoint != null ? req.RemoteEndPoint.Address.ToString() : "-";
                SafeLog($"{DateTime.Now:HH:mm:ss} | {req.HttpMethod} {req.RawUrl} | {status} | thr={thr} | {ms} ms | ip={ip}");
                try { res.Close(); } catch { /* ignore */ }
            }
        }

        
        private static readonly Regex WordRegex = new Regex(@"(?<=^|[^'\p{L}])(['\-]?\p{L}[\p{L}'\-]*)",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        private static (int uppercaseStartCount, int uniqueCount) AnalyzeDescription(string description)
        {
            if (string.IsNullOrWhiteSpace(description))
                return (0, 0);

            var matches = WordRegex.Matches(description);
            int upper = 0;

            var uniq = new System.Collections.Generic.HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (Match m in matches)
            {
                var w = m.Value;
                if (w.Length == 0) continue;

                var c = w[0];
                if (char.IsLetter(c) && char.IsUpper(c))
                    upper++;

                uniq.Add(w.ToLowerInvariant());
            }

            return (upper, uniq.Count);
        }

       

        private static string H(string s) => HttpUtility.HtmlEncode(s ?? "");

        private static string BuildBooksHtml(string author, int page, int limit, int total, System.Collections.Generic.IEnumerable<BookView> items)
        {
            var sb = new StringBuilder();
            sb.AppendLine("<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"/>");
            sb.AppendLine("<title>Google Books — Results</title>");
            sb.AppendLine("<style>");
            sb.AppendLine("body{font-family:Segoe UI,Arial,sans-serif;margin:2rem;line-height:1.5}");
            sb.AppendLine("table{border-collapse:collapse;width:100%;margin-top:1rem}");
            sb.AppendLine("th,td{border:1px solid #ddd;padding:.5rem;text-align:left;vertical-align:top}");
            sb.AppendLine("th{background:#f4f4f4}");
            sb.AppendLine(".muted{color:#666}");
            sb.AppendLine(".desc{white-space:pre-wrap}");
            sb.AppendLine("</style>");

            sb.AppendLine("<h1>Books</h1>");
            sb.AppendFormat("<p class=\"muted\">Author: <b>{0}</b> • Page: <b>{1}</b> • Limit: <b>{2}</b></p>", H(author), page, limit);
            sb.AppendFormat("<p>Total returned by API: <b>{0}</b></p>", total);

            sb.AppendLine("<table>");
            sb.AppendLine("<tr><th>Title</th><th>Uppercase words</th><th>Unique words</th><th>Description</th></tr>");

            foreach (var it in items)
            {
                var titleCell = string.IsNullOrWhiteSpace(it.InfoLink)
                    ? H(it.Title)
                    : $"<a href=\"{H(it.InfoLink)}\" target=\"_blank\" rel=\"noopener\">{H(it.Title)}</a>";

                sb.Append("<tr>");
                sb.AppendFormat("<td>{0}</td>", titleCell);
                sb.AppendFormat("<td>{0}</td>", it.UppercaseCount);
                sb.AppendFormat("<td>{0}</td>", it.UniqueCount);
                sb.AppendFormat("<td class=\"desc\">{0}</td>", H(it.Description));
                sb.Append("</tr>");
            }
            sb.AppendLine("</table>");

            sb.AppendLine("</html>");
            return sb.ToString();
        }

        private static string BuildLandingHtml()
        {
            return @"<!doctype html>
<html lang=""sr"">
<meta charset=""utf-8""/>
<title>Google Books Rx Server — P3</title>
<style>
 body { font-family: Segoe UI, Arial, sans-serif; margin: 2rem; line-height:1.5 }
 code { background:#f5f5f5; padding:.15rem .35rem; border-radius:.25rem }
 ul { margin-top:.75rem }
</style>

<h1>Google Books Rx Server — P3</h1>
<p>
  Endpointi: <code>/books</code> i <code>/health</code>. 
  Dodaj <code>&format=html</code> za HTML prikaz rezultata.
</p>

<h2>Primeri</h2>
<ul>
  <li><a href=""/books?author=tolkien&limit=5"">/books?author=tolkien&limit=5</a></li>
  <li><a href=""/books?author=tolkien&limit=5&format=html"">/books?author=tolkien&limit=5&format=html</a></li>
  <li><a href=""/books?author=asimov&page=2&limit=10"">/books?author=asimov&page=2&limit=10</a></li>
  <li><a href=""/health"">/health</a></li>
</ul>

</html>";
        }


        private static string BuildErrorHtml(string message)
        {
            string Hh(string s) => HttpUtility.HtmlEncode(s ?? "");
            return $@"<!doctype html>
<html lang=""en"">
<meta charset=""utf-8""/>
<title>Error</title>
<style>
 body{{font-family:Segoe UI,Arial,sans-serif;margin:2rem;line-height:1.5}}
 .alert{{background:#ffecec;border:1px solid #f5c2c7;padding:1rem;border-radius:.5rem;color:#842029}}
 a{{color:#0b5ed7;text-decoration:none}}
 a:hover{{text-decoration:underline}}
</style>
<h1>Request error</h1>
<p class=""alert"">{Hh(message)}</p>
<p><a href=""/"">&larr; Back</a></p>
</html>";
        }

       

        private static string BuildCacheKey(string author, int page, int limit, bool html)
        {
            author = (author ?? "").Trim().ToLowerInvariant();
            if (page < 1) page = 1;
            if (limit < 1) limit = 20;
            if (limit > 40) limit = 40;

            return $"books:author={author}&page={page}&limit={limit}&format={(html ? "html" : "json")}";
        }

       

        private sealed class BookView
        {
            public string Title { get; set; }
            public string Description { get; set; }
            public string InfoLink { get; set; }
            public int UppercaseCount { get; set; }
            public int UniqueCount { get; set; }
        }

      
        private static async Task WriteTextAsync(HttpListenerContext ctx, int status, string text, string contentType)
        {
            var buf = Encoding.UTF8.GetBytes(text ?? "");
            await WriteBytesAsync(ctx, status, buf, contentType + "; charset=utf-8");
        }

        private static Task WriteHtmlAsync(HttpListenerContext ctx, string html)
            => WriteTextAsync(ctx, 200, html, "text/html");

        private static Task WriteJsonAsync(HttpListenerContext ctx, object obj)
        {
            var json = JsonConvert.SerializeObject(obj, Formatting.Indented);
            var buf = Encoding.UTF8.GetBytes(json);
            return WriteBytesAsync(ctx, 200, buf, "application/json; charset=utf-8");
        }

        private static async Task WriteBytesAsync(HttpListenerContext ctx, int status, byte[] payload, string contentType)
        {
            ctx.Response.StatusCode = status;
            ctx.Response.ContentType = contentType;
            ctx.Response.ContentEncoding = Encoding.UTF8;
            ctx.Response.ContentLength64 = payload.LongLength;

            await ctx.Response.OutputStream.WriteAsync(payload, 0, payload.Length).ConfigureAwait(false);
            await ctx.Response.OutputStream.FlushAsync().ConfigureAwait(false);
        }

        private void SafeLog(string line)
        {
            lock (_consoleLock)
            {
                Console.WriteLine(line);
            }
        }

        public void Dispose()
        {
            _running = false;
            try { _listener.Stop(); } catch { }
            try { _acceptThread?.Join(1000); } catch { }
            _listener.Close();

            try { _incoming.OnCompleted(); } catch { }
            _subscription?.Dispose();
            _incoming?.Dispose();

            _gb.Dispose();
            _cache.Dispose();
        }
    }
}
