using Dapper;
using Npgsql;
using System.Diagnostics;

// 1. Connection String کو درست کیا (Database کا نام ایک لفظ ہونا چاہیے یا کوٹس میں)
const string connectionString = "Host=localhost; Port=5432; Database=optimizing; Username=postgres; Password=postgres";
const int recordCount = 100_000; // ٹیسٹ کے لیے ابھی کم رکھا ہے

// ڈیٹا بیس کی تیاری
await SetupDatabase(connectionString, recordCount);

// اپ ڈیٹس کی لسٹ بنانا
var updates = await BuildUpdateQueue(connectionString);

PrintSeperator();
Console.WriteLine($"Running benchmarks against {updates.Count} records...");
PrintSeperator();

// 2. باری باری تمام طریقوں کو چلانا
await ResetProcessedAsync(connectionString);
var sw = Stopwatch.StartNew();
await NaiveDapper(connectionString, updates);
sw.Stop();
Console.WriteLine($"[Approach 1] Naive Dapper: {sw.ElapsedMilliseconds} ms");

await ResetProcessedAsync(connectionString);
sw.Restart();
await DapperBatchedValues(connectionString, updates);
sw.Stop();
Console.WriteLine($"[Approach 2] Dapper Batched Values: {sw.ElapsedMilliseconds} ms");

await ResetProcessedAsync(connectionString);
sw.Restart();
await DapperUnnest(connectionString, updates);
sw.Stop();
Console.WriteLine($"[Approach 3] Dapper Unnest: {sw.ElapsedMilliseconds} ms");

await ResetProcessedAsync(connectionString);
sw.Restart();
await TempTableCopy(connectionString, updates);
sw.Stop();
Console.WriteLine($"[Approach 4] Temp Table Copy: {sw.ElapsedMilliseconds} ms");

// --- فنکشنز ---

static async Task SetupDatabase(string connectionString, int count)
{
    using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync();

    Console.WriteLine("Setting up schema...");
    await connection.ExecuteAsync("""
        DROP TABLE IF EXISTS orders;
        CREATE TABLE orders (
            id UUID NOT NULL PRIMARY KEY,
            customer_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            processed_at TIMESTAMPTZ
        );
        """);

    Console.WriteLine($"Seeding {count} orders...");
    await using var writer = await connection.BeginBinaryImportAsync(
        "COPY orders (id, customer_name, status, processed_at) FROM STDIN (FORMAT BINARY)");

    for (int i = 0; i < count; i++)
    {
        await writer.StartRowAsync();
        await writer.WriteAsync(Guid.NewGuid(), NpgsqlTypes.NpgsqlDbType.Uuid);
        await writer.WriteAsync($"Customer {i + 1}", NpgsqlTypes.NpgsqlDbType.Text);
        await writer.WriteAsync("pending", NpgsqlTypes.NpgsqlDbType.Text);
        await writer.WriteNullAsync();
    }
    await writer.CompleteAsync();
    Console.WriteLine("Schema ready.");
}

static async Task<List<OrderUpdate>> BuildUpdateQueue(string connectionString)
{
    await using var connection = new NpgsqlConnection(connectionString);
    // کالمز کے ناموں کو Id اور ProcessedAt سے میچ کرنے کے لیے ایلیس (Alias) استعمال کیا
    var records = (await connection.QueryAsync<dynamic>("SELECT id, customer_name FROM orders")).ToList();

    var baseTime = DateTime.UtcNow;
    return records.Select((r, i) => new OrderUpdate(r.id, baseTime.AddSeconds(i))).ToList();
}

static async Task ResetProcessedAsync(string connectionString)
{
    await using var connection = new NpgsqlConnection(connectionString);
    await connection.ExecuteAsync("UPDATE orders SET processed_at = NULL, status = 'pending'");
}

static async Task NaiveDapper(string connectionString, List<OrderUpdate> updates)
{
    await using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync();
    await using var transaction = await connection.BeginTransactionAsync();

    foreach (var update in updates)
    {
        await connection.ExecuteAsync(
            "UPDATE orders SET status = 'processed', processed_at = @ProcessedAt WHERE id = @Id",
            new { Id = update.Id, ProcessedAt = update.ProcessedAt },
            transaction: transaction
        );
    }
    await transaction.CommitAsync();
}

static async Task DapperBatchedValues(string connectionString, List<OrderUpdate> updates)
{
    // the max parameter size is 65535, and we have 2 parameters per record, so we can only update ~32k records at a time in this approach
    //if (updates .Count > 50_000)
    //{
    //    return;
    //}
    await using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync();
    await using var transaction = await connection.BeginTransactionAsync();

    var parmNames = string.Join(",", updates.Select((_, i) => $"(@Id{i}, @ProcessedAt{i}::timestamptz)"));
    var sql = $@"
        UPDATE orders SET status = 'processed', processed_at = v.processed_at
        FROM (VALUES {parmNames}) AS v(id, processed_at)
        WHERE orders.id = v.id::uuid";

    var parameters = new DynamicParameters();
    for (int i = 0; i < updates.Count; i++)
    {
        parameters.Add($"Id{i}", updates[i].Id);
        parameters.Add($"ProcessedAt{i}", updates[i].ProcessedAt);
    }

    await connection.ExecuteAsync(sql, parameters, transaction: transaction);
    await transaction.CommitAsync();
}

static async Task DapperUnnest(string connectionString, List<OrderUpdate> updates)
{
    await using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync();
    var ids = updates.Select(u => u.Id).ToArray();
    var processedAts = updates.Select(u => u.ProcessedAt).ToArray();

    await connection.ExecuteAsync("""
        UPDATE orders
        SET processed_at = v.processed_at, status = 'Processed'
        FROM UNNEST(@Ids, @ProcessedAts) AS v(id, processed_at)
        WHERE orders.id = v.id
        """, new { Ids = ids, ProcessedAts = processedAts });
}

static async Task TempTableCopy(string connectionString, List<OrderUpdate> updates)
{
    await using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync();
    await using var transaction = await connection.BeginTransactionAsync();

    await connection.ExecuteAsync("CREATE TEMP TABLE temp_updates(id UUID, processed_at TIMESTAMPTZ) ON COMMIT DROP", transaction: transaction);

    await using (var writer = await connection.BeginBinaryImportAsync("COPY temp_updates (id, processed_at) FROM STDIN (FORMAT BINARY)"))
    {
        foreach (var u in updates)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(u.Id, NpgsqlTypes.NpgsqlDbType.Uuid);
            await writer.WriteAsync(u.ProcessedAt, NpgsqlTypes.NpgsqlDbType.TimestampTz);
        }
        await writer.CompleteAsync();
    }

    await connection.ExecuteAsync("""
        UPDATE orders SET processed_at = temp_updates.processed_at, status = 'Processed'
        FROM temp_updates WHERE orders.id = temp_updates.id
        """, transaction: transaction);

    await transaction.CommitAsync();
}

static void PrintSeperator() => Console.WriteLine(new string('-', 80));

public record OrderUpdate(Guid Id, DateTime ProcessedAt);