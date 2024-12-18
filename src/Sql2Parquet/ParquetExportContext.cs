using System;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;

namespace Sql2Parquet
{
    using Parquet;
    using System.Threading;

    public class ParquetExportContext : IDisposable
    {
        private readonly DirectoryInfo _outputDir;
        private readonly (DefaultDbProviderFactory.Drivers, string) _connectionDetails;
        private readonly bool _isDisposable;
        private DbConnection _conn;

        public ParquetExportContext(string outputDir, string connectionString, DefaultDbProviderFactory.Drivers driver = DefaultDbProviderFactory.Drivers.MSSQL)
            : this(new DirectoryInfo(outputDir), connectionString, driver) { }

        public ParquetExportContext(DirectoryInfo outputDir, string connectionString, DefaultDbProviderFactory.Drivers driver = DefaultDbProviderFactory.Drivers.MSSQL)
        {
            _outputDir = outputDir;
            _connectionDetails = (driver, connectionString);
            _isDisposable = true;
        }   

        public ParquetExportContext(DirectoryInfo outputDir, DbConnection conn, bool disposable = false)
        {
            _conn = conn;
            _outputDir = outputDir;
            _isDisposable = disposable;
        }

        public async Task<FileInfo> Run(string name, string sqlText, CancellationToken cancellationToken)
        {
            if (!_outputDir.Exists)
            {
                _outputDir.Create();
            }

            string outputFilename = $"{name}.parquet";
            string outputPath = Path.Combine(_outputDir.FullName, outputFilename);

            var conn = await OpenConnection();

            using (var cmd = CreateDbCommand(conn, sqlText))
            using (DbDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                if (File.Exists(outputPath))
                {
                    File.Delete(outputPath);
                }

                using (var fileStream = File.OpenWrite(outputPath))
                {
                    var queryReader = new DbQueryReader(reader);
                    await ExportToParquet(queryReader, fileStream, cancellationToken);

                    return new FileInfo(outputPath);
                }
            }
        }

        private async Task<DbConnection> OpenConnection()
        {
            if (_conn == null)
            {
                if (_connectionDetails == default)
                {
                    throw new ArgumentNullException($"A valid DbConnection cannot be established: Connection string is undefined.");
                }

                var (driver, connStr) = _connectionDetails;
                _conn = await driver.OpenDbConnection(connStr);
            }

            if (_conn.State != System.Data.ConnectionState.Open)
            {
                await _conn.OpenAsync();
            }

            return _conn;
        }

        public static async Task ExportToParquet(IDbQueryReader reader, Stream outputStream, CancellationToken cancellationToken)
        {
            // Fetch column schema
            var columnSchema = await reader.GetColumnSchemaAsync();
            var rowGroup = RowGroupBuilder.CreateInstance(columnSchema);

            using (var writer = await ParquetWriter.CreateAsync(rowGroup.Schema, outputStream))
            {
                writer.CompressionMethod = CompressionMethod.Gzip;
                writer.CompressionLevel = System.IO.Compression.CompressionLevel.Optimal;

                int totalRows = reader.RecordsAffected;

                while (await reader.ReadAsync())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    rowGroup.AddRow(reader);

                    // flush 50k rows at a time.
                    if (rowGroup.RowCount == 50000)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        await rowGroup.WriteTo(writer);
                    }
                }

                // flush remaining rows to file.
                if (rowGroup.RowCount > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
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

        public void Dispose()
        {
            if (_isDisposable && _conn != null)
            {
                ((IDisposable)_conn).Dispose();
                _conn = null;
            }
        }
    }
}
