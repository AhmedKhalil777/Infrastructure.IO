using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text;


namespace Infrastructure.IO.InfluxDbClient
{
    public class InfluxDataRetriever : IDisposable
    {
        private HttpClient _httpClient;
        private string _influxUrl;
        private string _influxDataTable;

        /// <summary>
        /// A data retriever client
        /// </summary>
        /// <param name="influxUrl"></param>
        /// <param name="dataTable"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="timeout">default is 5 minutes</param>
        public InfluxDataRetriever(string influxUrl, string dataTable, string username, string password, TimeSpan timeout = default)
        {
            _influxUrl = influxUrl;
            _influxDataTable = dataTable;
            HttpClientHandler handler = new()
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
            };
            _httpClient = new HttpClient(handler)
            {
                Timeout = timeout == default ? TimeSpan.FromMinutes(5) : timeout,
            };
            if (string.IsNullOrEmpty(_influxDataTable) || string.IsNullOrEmpty(_influxUrl))
                return;
            if (!(string.IsNullOrWhiteSpace(username) && string.IsNullOrWhiteSpace(password)))
                _httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic",
                    Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));
            _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Accept-Encoding", "gzip, deflate");
            _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Accept", "application/json");
            _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Keep-Alive", "application/json");
        }


        internal async Task<IEnumerable<IRecord>> GetData(
         Func<string> prepareInfluxDbQuery,
         Func<IList<object>, IRecord> dataParser,
         CancellationTokenSource cts,
         Func<string> getFiltersMethod = null)
        {
            var splitDates = new List<Tuple<DateTime, DateTime>>();
            //cancel execution influxDB queries if query for one date range returns the exception (e.g. influxDB is down)
            var ctsInternal = new CancellationTokenSource();
            var ctsCombined = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, ctsInternal.Token);
            var taskCompletionSource = new TaskCompletionSource<bool>();
            ctsCombined.Token.Register(() =>
            {
                //taskCompletionSource.TrySetResult(-1);
                taskCompletionSource.TrySetCanceled();
            });
            var getDataTasks = new List<Task<IEnumerable<IRecord>>>();

            var query = prepareInfluxDbQuery();
            try
            {
                var task = Task.Run(() => DoGetData(prepareInfluxDbQuery, dataParser,
                    ctsCombined, getFiltersMethod), ctsCombined.Token);

                return await task.ConfigureAwait(false);

            }
            catch (OperationCanceledException)
            {
                //Console.WriteLine(e.Message);
                throw;
            }
            catch (AggregateException)
            {
                //Console.WriteLine(e.Message);
                throw;
            }
            catch (Exception)
            {
                //Console.WriteLine(e.Message);
                throw;
            }
        }

        private async Task<IEnumerable<IRecord>> DoGetData(
            Func<string> prepareInfluxDbQuery,
              Func<IList<object>, IRecord> dataParser,
            CancellationTokenSource cts, Func<string> getFiltersMethod = null)
        {


            var filters = getFiltersMethod != null ? getFiltersMethod() : "";
            var query = prepareInfluxDbQuery();

            cts.Token.ThrowIfCancellationRequested();
            var queryString = $@"db={_influxDataTable}&q={query}";// &epoch=s";
            var queryUrl = $@"{_influxUrl}/query?{queryString}&chunked=true";  //10000 points
            ConcurrentBag<IRecord> datas = new();

            //https://www.stevejgordon.co.uk/sending-and-receiving-json-using-httpclient-with-system-net-http-json
            var queryTask = Task.Run(async () =>
            {
                var serializer = new JsonSerializer
                {
                    DateFormatHandling = DateFormatHandling.MicrosoftDateFormat,
                    DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                    DateFormatString = "yyyy'-'MM'-'dd'T'HH':'mm':'ss.FFFFFFFFF'Z'"
                };
                //results are obtained in batches (10000 points per batch)
                Exception readException = null;

                Debug.WriteLine(queryUrl);

                var request = new HttpRequestMessage(HttpMethod.Get, new Uri(queryUrl));

                try
                {
                    using (var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token))
                    {
                        cts.Token.ThrowIfCancellationRequested();
                        if (response.IsSuccessStatusCode)
                        {
                            cts.Token.ThrowIfCancellationRequested();
                            var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                            using (var sr = new StreamReader(stream))
                            {
                                cts.Token.ThrowIfCancellationRequested();
                                using (var jr = new JsonTextReader(sr) { SupportMultipleContent = true })
                                {
                                    jr.DateParseHandling = DateParseHandling.None;
                                    while (jr.Read() && readException == null)
                                    {
                                        cts.Token.ThrowIfCancellationRequested();
                                        if (jr.TokenType != JsonToken.StartObject)
                                        {
                                            continue;
                                        }
                                        var root = serializer.Deserialize<InfluxDbResponseRoot>(jr);
                                        if (root.Results.Count > 0)
                                        {
                                            var series = root.Results.Where(p => p.Series != null).SelectMany(p => p.Series);
                                            if (series != null && series.Any())
                                            {
                                                var vals = series.SelectMany(p => p.Values);

                                                //Parallel.ForEach(vals, v =>
                                                foreach (var v in vals)
                                                {
                                                    cts.Token.ThrowIfCancellationRequested();
                                                    var data = dataParser(v);
                                                    if (data != null)
                                                        datas.Add(data);
                                                }; //);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    //webException, influxDB is down?
                    cts.Cancel(true);
                    throw;
                }
                cts.Token.ThrowIfCancellationRequested();
                return datas;
            }, cts.Token).ConfigureAwait(false);
            return await queryTask;
        }


        public void Dispose()
        {
            _httpClient.Dispose();
        }
    }
}
