using System;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;

namespace Sql2Parquet
{
    using Parquet;

    public class ParquetExportContext
    {
        private readonly DirectoryInfo _outputDir;

        public ParquetExportContext(DirectoryInfo outputDir)
        {
            _outputDir = outputDir; 
        }

        public async Task<FileInfo> StartAsync(string name, string sqlText, DbConnection conn)
        {
            string outputFilename = $"{name}.parquet";
            string outputPath = Path.Combine(_outputDir.FullName, outputFilename);

            if (File.Exists(outputPath))
            {
                File.Delete(outputPath);
            }

            using (var cmd = CreateDbCommand(conn, sqlText))
            using (DbDataReader reader = await cmd.ExecuteReaderAsync())
            using (var fileStream = File.OpenWrite(outputPath))
            {
                var queryReader = new DbQueryReader(reader);
                await ExportToParquet(queryReader, fileStream);

                return new FileInfo(outputPath);
            }   
        }

        public static async Task ExportToParquet(IDbQueryReader reader, Stream outputStream)
        {
            // Fetch column schema
            var columnSchema = await reader.GetColumnSchemaAsync();
            var rowGroup = RowGroupBuilder.CreateInstance(columnSchema);

            using (var writer = await ParquetWriter.CreateAsync(rowGroup.Schema, outputStream))
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

        private static DbCommand CreateDbCommand(DbConnection conn, string sqlText)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = sqlText;
            cmd.CommandTimeout = (int)TimeSpan.FromMinutes(2).TotalSeconds;

            return cmd;
        }
    }
}
