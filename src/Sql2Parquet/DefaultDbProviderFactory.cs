using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sql2Parquet
{
    public static class DefaultDbProviderFactory
    {
        public enum Drivers
        {
            MSSQL = 0,
            PSQL = 1
        }

        static DefaultDbProviderFactory()
        {   
            DbProviderFactories.RegisterFactory(Invariant(Drivers.MSSQL), Microsoft.Data.SqlClient.SqlClientFactory.Instance);
            DbProviderFactories.RegisterFactory(Invariant(Drivers.PSQL), Npgsql.NpgsqlFactory.Instance);
        }

        public static string Invariant(Drivers driver)
        {
            switch (driver)
            {
                case Drivers.MSSQL: 
                    return "Microsoft.Data.SqlClient";
                case Drivers.PSQL:
                    return "Npgsql";
                default:
                    throw new NotSupportedException($"Unsupported database driver: {driver.ToString()}");
            }
        }

        public static async Task<DbConnection> OpenDbConnection(string connectionString, Drivers driver = Drivers.MSSQL)
        {
            string invariantName = Invariant(driver);
            var factory = DbProviderFactories.GetFactory(invariantName);
            var conn = factory.CreateConnection();

            conn.ConnectionString = connectionString;

            await conn.OpenAsync();

            return conn;
        }

        public static Task<DbConnection> OpenDbConnection(this Drivers driver,  string connectionString)
        {
            return OpenDbConnection(connectionString, driver);
        }
    }
}
