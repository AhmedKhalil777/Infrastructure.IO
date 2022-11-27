using Newtonsoft.Json;

namespace Infrastructure.IO.InfluxDbClient;

public struct InfluxDbResponseRoot
{
    [JsonProperty("results")]
    public List<InfluxDbResponseResult> Results { get; set; }
}

public struct InfluxDbResponseResult
{
    [JsonProperty("statement_id")]
    public int StatementId { get; set; }
    [JsonProperty("series")]
    public List<InfluxDbResponseSeries> Series { get; set; }
}
public struct InfluxDbResponseSeries
{
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("columns")]
    public List<string> Columns { get; set; }
    [JsonProperty("values")]
    public List<List<object>> Values { get; set; }
}