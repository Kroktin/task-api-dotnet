using System.Text.Json.Serialization;

namespace TaskApi;

[JsonConverter(typeof(JsonStringEnumConverter<Priority>))]
public enum Priority
{
    Low,
    Medium,
    High
}

public class TaskItem
{
    public const int TitleMaxLength = 200;

    public Guid Id { get; set; }
    public string Title { get; set; } = null!;
    public bool IsCompleted { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
    public Priority Priority { get; set; } = Priority.Medium;

    // Системная колонка Postgres xmin: меняется при каждом UPDATE строки.
    // EF кладет ее в WHERE при сохранении, это и есть RowVersion без своей колонки.
    [JsonIgnore]
    public uint Version { get; set; }

    // Завершение только здесь, чтобы CompletedAt нельзя было проставить в обход.
    public void Complete(DateTimeOffset now)
    {
        IsCompleted = true;
        CompletedAt = now;
    }
}

public record CreateTaskRequest(string? Title, Priority? Priority);

public record TaskCompletedEvent(Guid TaskId, string Title, DateTimeOffset CompletedAt, Priority Priority);
