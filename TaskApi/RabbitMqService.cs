using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace TaskApi;

public class RabbitMqOptions
{
    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string Exchange { get; set; } = "task.events";
    public int ShutdownTimeoutSeconds { get; set; } = 5;
}

public interface ITaskEventPublisher
{
    // Не ждет брокера: кладет событие в очередь в памяти и сразу возвращается.
    void PublishTaskCompleted(TaskCompletedEvent evt);
}

/// <summary>
/// Публикует события в RabbitMQ из фона. Эндпоинт пишет в Channel и отвечает клиенту,
/// отправку делает ExecuteAsync. При остановке дописывает очередь (не дольше ShutdownTimeoutSeconds)
/// и закрывает канал с соединением.
/// </summary>
public sealed class RabbitMqService(IOptions<RabbitMqOptions> options, ILogger<RabbitMqService> logger)
    : BackgroundService, ITaskEventPublisher
{
    public const string TaskCompletedRoutingKey = "task.completed";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly RabbitMqOptions _options = options.Value;

    // HACK: очередь в памяти, при падении процесса неотправленное теряется.
    // В проде сделал бы outbox-таблицу в той же транзакции, что и завершение задачи.
    private readonly Channel<TaskCompletedEvent> _queue = Channel.CreateBounded<TaskCompletedEvent>(
        new BoundedChannelOptions(10_000) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });

    private IConnection? _connection;
    private IChannel? _channel;

    public void PublishTaskCompleted(TaskCompletedEvent evt)
    {
        if (!_queue.Writer.TryWrite(evt))
            logger.LogWarning("Очередь публикации закрыта, событие task.completed для {TaskId} не отправлено", evt.TaskId);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // stoppingToken тут не используем: на остановке Writer закрывается в StopAsync,
        // а цикл дочитывает то, что уже лежит в очереди.
        await foreach (var evt in _queue.Reader.ReadAllAsync(CancellationToken.None))
            await SendAsync(evt);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete();

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(_options.ShutdownTimeoutSeconds));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            if (ExecuteTask is not null)
                await ExecuteTask.WaitAsync(linked.Token);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Не успели отправить {Count} событий за {Seconds} с до остановки",
                _queue.Reader.Count, _options.ShutdownTimeoutSeconds);
        }

        await CloseAsync();
    }

    private async Task SendAsync(TaskCompletedEvent evt)
    {
        try
        {
            var channel = await GetChannelAsync();
            var body = JsonSerializer.SerializeToUtf8Bytes(evt, JsonOptions);
            var props = new BasicProperties { ContentType = "application/json", DeliveryMode = DeliveryModes.Persistent };
            await channel.BasicPublishAsync(_options.Exchange, TaskCompletedRoutingKey, mandatory: false, props, body);
        }
        catch (Exception ex)
        {
            // Fail silently: задача в БД уже завершена, событие только логируем.
            // HACK: без ретраев, доделал бы через outbox + повтор с backoff.
            logger.LogError(ex, "Не удалось отправить task.completed в RabbitMQ: {Payload}",
                JsonSerializer.Serialize(evt, JsonOptions));
            await CloseAsync();
        }
    }

    private async Task<IChannel> GetChannelAsync()
    {
        if (_channel is { IsOpen: true })
            return _channel;

        await CloseAsync();
        var factory = new ConnectionFactory
        {
            HostName = _options.HostName,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            RequestedConnectionTimeout = TimeSpan.FromSeconds(3),
        };
        _connection = await factory.CreateConnectionAsync("task-api");
        _channel = await _connection.CreateChannelAsync();
        await _channel.ExchangeDeclareAsync(_options.Exchange, ExchangeType.Topic, durable: true);
        return _channel;
    }

    private async Task CloseAsync()
    {
        try
        {
            if (_channel is not null) await _channel.CloseAsync();
            if (_connection is not null) await _connection.CloseAsync();
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Ошибка при закрытии соединения с RabbitMQ");
        }
        finally
        {
            _channel?.Dispose();
            _connection?.Dispose();
            _channel = null;
            _connection = null;
        }
    }
}
