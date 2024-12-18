using Spectre.Console;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using static Sql2Parquet.Command;

namespace Sql2Parquet
{
    public class ExportTask
    {
        public ExportTask(DirectoryInfo outputDir, string name, string sqlText, Args args, ProgressContext progress)
        {
            Name = name;
            SqlText = sqlText;
            Context = new ParquetExportContext(outputDir, args.ConnectionString, args.Driver);

            ProgressTask = progress.AddTask($"[gray]{name}[/]");
            ProgressTask.IsIndeterminate = true;
        }

        public string Name { get; }
        public string SqlText { get; }
        public ProgressTask ProgressTask { get; }
        public ParquetExportContext Context { get; }
        public Task<FileInfo> Task { get; private set; }
        public bool IsFinished => (Task?.IsCompleted).GetValueOrDefault();

        public ExportTask Start(CancellationToken cancellationToken)
        {
            Task = Context.Run(Name, SqlText, cancellationToken);
            return this;
        }

        public void SetFinished()
        {
            ProgressTask.Description($"[green]{Name} OK[/]");
            ProgressTask.Increment(100);
            ProgressTask.StopTask();
        }

        public void SetError(string message)
        {
            ProgressTask.Description($"[red]{Name} Failed. {message}[/]");
            ProgressTask.IsIndeterminate = false;
            ProgressTask.StopTask();
        }
    }
}
