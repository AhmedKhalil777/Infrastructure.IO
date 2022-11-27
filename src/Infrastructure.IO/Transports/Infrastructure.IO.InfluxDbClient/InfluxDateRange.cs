namespace Infrastructure.IO.InfluxDbClient;

public record struct InfluxDateRange(DateTime From , DateTime To, TimeSpan SplitBy);
