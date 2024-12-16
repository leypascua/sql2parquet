using Microsoft.Data.SqlClient;
using Parquet;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Sql2Parquet
{
    public static class Command
    {
        public static async Task<int> Execute(Args args)
        {
            var parquetFiles = new ConcurrentBag<string>();

            foreach (var query in args.Queries)
            {
                string tempResultPath = await ExportToParquet(args.ConnectionString, query, args.TempPath);
                parquetFiles.Add(tempResultPath);
            }

            // move all generated parquet files to final destination
            foreach (var filename in parquetFiles)
            {
                var newParquetFile = new FileInfo(filename);
                string newPath = Path.Combine(args.OutputPath, newParquetFile.Name);

                MoveOutput(newParquetFile, newPath, args.FilesToKeep);                
                Directory.Delete(newParquetFile.Directory.FullName);
            }

            return 0;
        }

        private static void MoveOutput(FileInfo sourceFile, string newPath, int filesToKeep)
        {
            if (File.Exists(newPath))
            {
                var existingFile = new FileInfo(newPath);
                string renamed = $"{Path.GetFileNameWithoutExtension(existingFile.Name)}.{existingFile.LastWriteTimeUtc.ToString("yyyyMMddTHHmmss")}{Path.GetExtension(existingFile.Name)}";
                string renamedPath = Path.Combine(existingFile.Directory.FullName, renamed);
                File.Move(existingFile.FullName, renamedPath);
            }

            File.Move(sourceFile.FullName, newPath);

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

        private static async Task<string> ExportToParquet(string connectionString, KeyValuePair<string, string> query, string tempPath)
        {
            string outputFilename = $"{query.Key}.parquet";
            string outputPath = Path.Combine(tempPath, outputFilename);

            using (var conn = new SqlConnection(connectionString))
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = query.Value;
                conn.Open();

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    // Fetch column schema
                    var columnSchema = await reader.GetColumnSchemaAsync();
                    var rowGroup = RowGroupBuilder.CreateInstance(columnSchema);

                    using (Stream fileStream = File.OpenWrite(outputPath))
                    using (var writer = await ParquetWriter.CreateAsync(rowGroup.Schema, fileStream))
                    {
                        writer.CompressionMethod = CompressionMethod.Gzip;
                        writer.CompressionLevel = System.IO.Compression.CompressionLevel.Optimal;

                        while (await reader.ReadAsync())
                        {
                            rowGroup.AddRow(reader);

                            // flush 50k rows at a time.
                            if (rowGroup.RowCount == 50000)
                            {
                                await rowGroup.WriteTo(writer);
                            }
                        }

                        // flush remaining rows to file.
                        if (rowGroup.RowCount > 0)
                        {
                            await rowGroup.WriteTo(writer);
                        }
                    }
                }
            }

            return outputPath;
        }
        
        public class Args
        {
            public Args()
            {
                Queries = new Dictionary<string, string>();
                RequestDate = DateTimeOffset.UtcNow;
                FilesToKeep = 5;
            }

            public string ConnectionString { get; set; }
            public IDictionary<string, string> Queries { get; set; }
            public DateTimeOffset RequestDate { get; set; }
            public string OutputPath { get; set; }
            public string TempPath { get; set; }
            public int FilesToKeep { get; set; }
        }
    }
}
