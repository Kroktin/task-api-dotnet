using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;

namespace TaskApi.Tests;

/// <summary>
/// Поднимает настоящие Postgres и RabbitMQ в контейнерах (нужен Docker),
/// приложение запускается через WebApplicationFactory.
/// </summary>
public class CompleteTaskIntegrationTest : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder().WithImage("postgres:16-alpine").Build();
    private readonly RabbitMqContainer _rabbit = new RabbitMqBuilder().WithImage("rabbitmq:4-alpine").Build();

    private WebApplicationFactory<Program> _factory = null!;
    private HttpClient _client = null!;

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_postgres.StartAsync(), _rabbit.StartAsync());

        var rabbitUri = new Uri(_rabbit.GetConnectionString());
        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("ConnectionStrings:Postgres", _postgres.GetConnectionString());
            b.UseSetting("RabbitMq:HostName", rabbitUri.Host);
            b.UseSetting("RabbitMq:Port", rabbitUri.Port.ToString());
            b.UseSetting("RabbitMq:UserName", RabbitMqBuilder.DefaultUsername);
            b.UseSetting("RabbitMq:Password", RabbitMqBuilder.DefaultPassword);
        });
        _client = _factory.CreateClient();
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _factory.DisposeAsync();
        await Task.WhenAll(_postgres.DisposeAsync().AsTask(), _rabbit.DisposeAsync().AsTask());
    }

    [Fact]
    public async Task Complete_PublishesTaskCompletedEvent()
    {
        await using var consumer = await TestConsumer.StartAsync(_rabbit.GetConnectionString());

        // 1. Создать задачу
        var createResponse = await _client.PostAsJsonAsync("/tasks", new { title = "Buy milk", priority = "High" });
        Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);
        var created = await createResponse.Content.ReadFromJsonAsync<JsonElement>();
        var id = created.GetProperty("id").GetGuid();
        Assert.False(created.GetProperty("isCompleted").GetBoolean());
        Assert.Equal(JsonValueKind.Null, created.GetProperty("completedAt").ValueKind);

        // 2. Завершить
        var completeResponse = await _client.PutAsync($"/tasks/{id}/complete", null);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        // 3. RabbitMQ получил сообщение
        var message = await consumer.ReceiveAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("task.completed", message.RoutingKey);
        var body = JsonDocument.Parse(message.Body).RootElement;
        Assert.Equal(id, body.GetProperty("taskId").GetGuid());
        Assert.Equal("Buy milk", body.GetProperty("title").GetString());
        Assert.Equal("High", body.GetProperty("priority").GetString());
        Assert.True(body.TryGetProperty("completedAt", out _));
    }

    [Fact]
    public async Task Complete_ParallelRequests_OneOkRestConflict()
    {
        var createResponse = await _client.PostAsJsonAsync("/tasks", new { title = "Race" });
        var id = (await createResponse.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("id").GetGuid();

        var responses = await Task.WhenAll(Enumerable.Range(0, 10)
            .Select(_ => _client.PutAsync($"/tasks/{id}/complete", null)));

        Assert.Equal(1, responses.Count(r => r.StatusCode == HttpStatusCode.OK));
        Assert.Equal(9, responses.Count(r => r.StatusCode == HttpStatusCode.Conflict));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public async Task Create_InvalidTitle_Returns400(string? title)
    {
        var response = await _client.PostAsJsonAsync("/tasks", new { title });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Create_TooLongTitle_Returns400()
    {
        var response = await _client.PostAsJsonAsync("/tasks", new { title = new string('a', 201) });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Theory]
    [InlineData("\"Ultra\"")]
    [InlineData("7")]
    public async Task Create_InvalidPriority_Returns400(string priorityJson)
    {
        var content = new StringContent($"{{\"title\":\"a\",\"priority\":{priorityJson}}}", Encoding.UTF8, "application/json");
        var response = await _client.PostAsync("/tasks", content);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    private sealed record ReceivedMessage(string RoutingKey, string Body);

    /// <summary>Временная очередь, привязанная к task.events / task.completed.</summary>
    private sealed class TestConsumer : IAsyncDisposable
    {
        private readonly IConnection _connection;
        private readonly IChannel _channel;
        private readonly Channel<ReceivedMessage> _received = Channel.CreateUnbounded<ReceivedMessage>();

        private TestConsumer(IConnection connection, IChannel channel)
        {
            _connection = connection;
            _channel = channel;
        }

        public static async Task<TestConsumer> StartAsync(string connectionString)
        {
            var connection = await new ConnectionFactory { Uri = new Uri(connectionString) }.CreateConnectionAsync();
            var channel = await connection.CreateChannelAsync();
            await channel.ExchangeDeclareAsync("task.events", ExchangeType.Topic, durable: true);
            var queue = await channel.QueueDeclareAsync();
            await channel.QueueBindAsync(queue.QueueName, "task.events", "task.completed");

            var consumer = new TestConsumer(connection, channel);
            var basic = new AsyncEventingBasicConsumer(channel);
            basic.ReceivedAsync += async (_, ea) =>
                await consumer._received.Writer.WriteAsync(
                    new ReceivedMessage(ea.RoutingKey, Encoding.UTF8.GetString(ea.Body.Span)));
            await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, basic);
            return consumer;
        }

        public async Task<ReceivedMessage> ReceiveAsync(TimeSpan timeout)
        {
            using var cts = new CancellationTokenSource(timeout);
            return await _received.Reader.ReadAsync(cts.Token);
        }

        public async ValueTask DisposeAsync()
        {
            await _channel.CloseAsync();
            await _connection.CloseAsync();
            _channel.Dispose();
            _connection.Dispose();
        }
    }
}
