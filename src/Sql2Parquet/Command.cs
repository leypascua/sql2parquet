using Microsoft.Data.SqlClient;
using Parquet;
using Spectre.Console;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static Sql2Parquet.Command;

namespace Sql2Parquet
{
    public static class Command
    {
        public static async Task<int> Execute(Args args, CancellationToken cancellationToken)
        {
            var parquetFiles = new ConcurrentBag<FileInfo>();
            var tempDir = new DirectoryInfo(Path.Combine(args.TempPath, Guid.NewGuid().ToString()));

            if (!tempDir.Exists)
            {
                tempDir.Create();
            }

            await AnsiConsole.Progress()
                .AutoRefresh(true)
                .AutoClear(false)
                .HideCompleted(false)
                .Columns([
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new SpinnerColumn()
                ])
                .StartAsync(async (ctx) =>
                {
                    // Define tasks
                    var tasks = StartTasks(tempDir, args, ctx, cancellationToken)
                        .ToDictionary(t => t.Task.Id);

                    while (tasks.Any())
                    {   
                        var completedTask = await Task.WhenAny(tasks.Values.Select(t => t.Task));
                        var exportTask = tasks[completedTask.Id];

                        try
                        {
                            FileInfo tempParquetFile = await completedTask;

                            if (tempParquetFile.Exists)
                            {
                                parquetFiles.Add(tempParquetFile);
                            }

                            exportTask.SetFinished();
                        }
                        catch (OperationCanceledException)
                        {
                            exportTask.SetError("Operation cancelled.");
                        }
                        catch (Exception ex)
                        {
                            exportTask.SetError(ex.Message);
                        }

                        tasks.Remove(completedTask.Id);
                    }
                });

            AnsiConsole.Console.WriteLine("Finalizing...");
            // move all generated parquet files to final destination
            foreach (var parquetFile in parquetFiles)
            {
                string newPath = Path.Combine(args.OutputPath, parquetFile.Name);

                MoveOutput(parquetFile, newPath);
                PruneOldFiles(newPath, args.FilesToKeep);
            }

            AnsiConsole.Console.WriteLine("Cleaning up");
            tempDir.Delete();
            AnsiConsole.Console.WriteLine("Done.");

            return 0;
        }

        private static IEnumerable<ExportTask> StartTasks(DirectoryInfo tempDir, Args args, ProgressContext progress, CancellationToken cancellationToken)
        {
            foreach (var query in args.Queries)
            {
                yield return new ExportTask(tempDir, query.Key, query.Value, args, progress)
                    .Start(cancellationToken); 
            }
        }

        private static void MoveOutput(FileInfo sourceFile, string newPath)
        {
            if (File.Exists(newPath))
            {
                var existingFile = new FileInfo(newPath);
                string renamed = $"{Path.GetFileNameWithoutExtension(existingFile.Name)}.{existingFile.LastWriteTimeUtc.ToString("yyyyMMddTHHmmss")}{Path.GetExtension(existingFile.Name)}";
                string renamedPath = Path.Combine(existingFile.Directory.FullName, renamed);
                File.Move(existingFile.FullName, renamedPath);
            }

            File.Move(sourceFile.FullName, newPath);
        }

        private static void PruneOldFiles(string newPath, int filesToKeep)
        {
            // prune old files
            var newFile = new FileInfo(newPath);
            string searchPattern = $"{Path.GetFileNameWithoutExtension(newFile.Name)}.*{Path.GetExtension(newFile.Name)}";
            var relevantFiles = newFile.Directory.GetFileSystemInfos(searchPattern)
                .OrderByDescending(fi => fi.LastWriteTimeUtc)
                .Skip(filesToKeep)
                .ToList();

            foreach (var file in relevantFiles)
            {
                file.Delete();
            }
        }

        public class Args
        {
            public Args()
            {
                Queries = new Dictionary<string, string>();
                RequestDate = DateTimeOffset.UtcNow;
                FilesToKeep = 5;
                Driver = DefaultDbProviderFactory.Drivers.MSSQL;
            }

            public string ConnectionString { get; set; }
            public IDictionary<string, string> Queries { get; set; }
            public DateTimeOffset RequestDate { get; set; }
            public string OutputPath { get; set; }
            public string TempPath { get; set; }
            public int FilesToKeep { get; set; }
            public DefaultDbProviderFactory.Drivers Driver { get; set; }
        }
    }
}
