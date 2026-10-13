using Microsoft.EntityFrameworkCore;

namespace TaskApi;

public class TaskDbContext(DbContextOptions<TaskDbContext> options) : DbContext(options)
{
    public DbSet<TaskItem> Tasks => Set<TaskItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var task = modelBuilder.Entity<TaskItem>();
        task.ToTable("tasks");
        task.HasKey(t => t.Id);
        task.Property(t => t.Title).IsRequired().HasMaxLength(TaskItem.TitleMaxLength);
        task.Property(t => t.Priority).HasConversion<string>().HasMaxLength(16);
        task.Property(t => t.Version).IsRowVersion();
        task.HasIndex(t => t.CreatedAt);
    }
}
