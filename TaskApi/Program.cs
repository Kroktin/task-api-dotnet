using Microsoft.EntityFrameworkCore;
using TaskApi;

var builder = WebApplication.CreateBuilder(args);

#region Services

builder.Services.AddDbContext<TaskDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection("RabbitMq"));
builder.Services.AddSingleton<RabbitMqService>();
builder.Services.AddSingleton<ITaskEventPublisher>(sp => sp.GetRequiredService<RabbitMqService>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<RabbitMqService>());

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddProblemDetails();
// В Development ошибка биндинга (кривой JSON, неизвестный enum) по умолчанию летит исключением
// и превращается в 500. Отключаем, чтобы клиент всегда получал 400.
builder.Services.Configure<RouteHandlerOptions>(o => o.ThrowOnBadRequest = false);

#endregion

var app = builder.Build();

app.UseExceptionHandler();
app.UseStatusCodePages();

// HACK: миграции катим на старте, для тестового ок. В проде вынес бы в отдельный шаг деплоя
// (dotnet ef migrations bundle), чтобы несколько инстансов не мигрировали одновременно.
using (var scope = app.Services.CreateScope())
    await scope.ServiceProvider.GetRequiredService<TaskDbContext>().Database.MigrateAsync();

#region Endpoints

var tasks = app.MapGroup("/tasks");

tasks.MapPost("/", async (CreateTaskRequest request, TaskDbContext db, TimeProvider clock) =>
{
    var title = request.Title?.Trim();
    if (string.IsNullOrEmpty(title) || title.Length > TaskItem.TitleMaxLength)
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["title"] = [$"Title не должен быть пустым и длиннее {TaskItem.TitleMaxLength} символов"]
        });

    if (request.Priority is { } p && !Enum.IsDefined(p))
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["priority"] = ["Допустимо: Low, Medium, High"]
        });

    var task = new TaskItem
    {
        Id = Guid.NewGuid(),
        Title = title,
        CreatedAt = clock.GetUtcNow(),
        Priority = request.Priority ?? Priority.Medium,
    };
    db.Tasks.Add(task);
    await db.SaveChangesAsync();

    return Results.Created($"/tasks/{task.Id}", task);
});

// Пагинация опциональная: без параметров отдаем все, как в ТЗ.
tasks.MapGet("/", async (TaskDbContext db, int? skip, int? take) =>
{
    if (skip < 0 || take is < 1 or > 1000)
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["paging"] = ["skip >= 0, take от 1 до 1000"]
        });

    IQueryable<TaskItem> query = db.Tasks.AsNoTracking().OrderByDescending(t => t.CreatedAt).ThenBy(t => t.Id);
    if (skip is { } s) query = query.Skip(s);
    if (take is { } t) query = query.Take(t);

    return Results.Ok(await query.ToListAsync());
});

tasks.MapPut("/{id:guid}/complete", async (Guid id, TaskDbContext db, TimeProvider clock, ITaskEventPublisher publisher) =>
{
    var task = await db.Tasks.FindAsync(id);
    if (task is null)
        return Results.NotFound();

    // Быстрый путь: уже завершена до нас.
    if (task.IsCompleted)
        return Results.Conflict(new { error = "Задача уже завершена" });

    task.Complete(clock.GetUtcNow());
    try
    {
        // UPDATE ... WHERE id = @id AND xmin = @version. Если параллельный запрос
        // успел раньше, xmin другой, строк обновлено 0, EF кидает исключение.
        await db.SaveChangesAsync();
    }
    catch (DbUpdateConcurrencyException)
    {
        return Results.Conflict(new { error = "Задача уже завершена" });
    }

    publisher.PublishTaskCompleted(new TaskCompletedEvent(task.Id, task.Title, task.CompletedAt!.Value, task.Priority));
    return Results.Ok(task);
});

tasks.MapDelete("/{id:guid}", async (Guid id, TaskDbContext db) =>
{
    var deleted = await db.Tasks.Where(t => t.Id == id).ExecuteDeleteAsync();
    return deleted == 0 ? Results.NotFound() : Results.NoContent();
});

#endregion

app.Run();

public partial class Program;
