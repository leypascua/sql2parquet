using Microsoft.Data.SqlClient;
using Parquet;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Sql2Parquet
{
    public static class Command
    {
        public static async Task<int> Execute(Args args)
        {
            var tempDir = new DirectoryInfo(args.TempPath);
            var parquetFiles = new ConcurrentBag<FileInfo>();

            foreach (var query in args.Queries)
            {
                var exportContext = new ParquetExportContext(tempDir);

                using (var conn = await DefaultDbProviderFactory.OpenDbConnection(args.ConnectionString))
                {
                    var tempResult = await exportContext.StartAsync(name: query.Key, sqlText: query.Value, conn);
                    parquetFiles.Add(tempResult);
                }
            }

            // move all generated parquet files to final destination
            foreach (var parquetFile in parquetFiles)
            {
                string newPath = Path.Combine(args.OutputPath, parquetFile.Name);

                MoveOutput(parquetFile, newPath);
                PruneOldFiles(newPath, args.FilesToKeep);
                Directory.Delete(parquetFile.Directory.FullName);
            }

            return 0;
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
