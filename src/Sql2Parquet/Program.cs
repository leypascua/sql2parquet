﻿using System;
using System.Collections.Generic;
using System.CommandLine;
using System.IO;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Tasks;

namespace Sql2Parquet
{
    public class Program
    {
        static async Task<int> Main(string[] args)
        {
            var rootCommand = CreateCommand();

            return await rootCommand.InvokeAsync(args);
        }

        private static RootCommand CreateCommand()
        {
            // sql2parquet --connect "Server=.\MSSQL2019; Database=HP_Muppet_Reporting_Dev; Integrated Security=true; Trusted Connection=true;" --query "/path/to/query/dir/*.sql" --output "/path/to/parquet/output"

            var rootCommand = new RootCommand("sql2parquet: Export results of SQL queries to Apache Parquet files");

            var connectionStringOption = new Option<string>(
                name: "--connection",                
                description: "The SQL Server connection string to use."                
            )
            { IsRequired = true, ArgumentHelpName = "connection string" };            
            rootCommand.AddOption(connectionStringOption);

            var queryPathOption = new Option<string>(
                name: "--query",
                description: $"The path to text file(s) containing the SQL query to execute. Defaults to using [*.sql] files in the current working directory [{Environment.CurrentDirectory}]."
            )
            { ArgumentHelpName = "/path/to/*.sql" };
            rootCommand.AddOption(queryPathOption);

            var outputPathOption = new Option<string>(
                name: "--output",
                description: "The path where Apache Parquet files will be written to. Defaults to using the current working directory."
            );
            rootCommand.AddOption(outputPathOption);

            var tempPathOption = new Option<string>(
                name: "--temp",
                description: "The path where files in-progress will be temporarily written to before it is moved to the final output path."
            );
            rootCommand.AddOption(tempPathOption);

            rootCommand.SetHandler(async (context) =>
            {
                string connectionString = context.ParseResult.GetValueForOption(connectionStringOption);
                string queryPath = context.ParseResult.GetValueForOption(queryPathOption);
                string outputPath = context.ParseResult.GetValueForOption(outputPathOption);
                string tempPath = context.ParseResult.GetValueForOption(tempPathOption);
                var args = await CreateArgs(connectionString, queryPath, outputPath, tempPath);

                context.ExitCode = await Command.Execute(args);
            });

            return rootCommand;
        }

        private static async Task<Command.Args> CreateArgs(string connectionString, string queryPath, string outputPath, string tempPath)
        {
            return new Command.Args
            {
                ConnectionString = connectionString,
                Queries = await ResolveQueriesFrom(queryPath),
                OutputPath = ResolveValidDir(outputPath, Path.Combine(Environment.CurrentDirectory, ".output")),
                TempPath = ResolveValidDir(tempPath, Path.Combine(Environment.CurrentDirectory, ".temp", Guid.NewGuid().ToString()))
            };
        }

        private static string ResolveValidDir(params string[] paths)
        {
            foreach (string path in paths)
            {
                if (string.IsNullOrEmpty(path)) continue;

                var dinfo = new DirectoryInfo(path);

                if (!dinfo.Exists)
                {
                    dinfo.Create();
                }

                return dinfo.FullName;
            }

            return Path.Combine(Path.GetTempPath(), ".sql2parquet", Guid.NewGuid().ToString());
        }

        private static async Task<IDictionary<string, string>> ResolveQueriesFrom(string queryPath)
        {
            var queryFiles = ListFilesFrom(queryPath);

            var result = new Dictionary<string, string>();
            foreach (string filename in queryFiles)
            {
                if (File.Exists(filename))
                {
                    string name = Path.GetFileNameWithoutExtension(filename)
                        .Replace(".query", string.Empty)
                        .Replace(".", "_");

                    result.Add(name, await ReadSqlText(filename));
                }
            }

            return result;
        }

        private static async Task<string> ReadSqlText(string filename)
        {
            using (var fs = File.OpenText(filename))
            {
                return await fs.ReadToEndAsync();
            }
        }

        private static IEnumerable<string> ListFilesFrom(string queryPath)
        {
            // if file exists, use it.
            if (File.Exists(queryPath))
            {
                yield return queryPath;
            }

            // if query path is a directory, get all .sql files from it
            if (Directory.Exists(queryPath))
            {
                foreach (string sqlFile in Directory.GetFiles(queryPath, "*.sql"))
                {
                    yield return sqlFile;
                }
            }

            yield break;
        }
    }
}