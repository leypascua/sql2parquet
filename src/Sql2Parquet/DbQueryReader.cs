using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sql2Parquet
{
    public interface IDbQueryReader
    {
        Task<bool> ReadAsync();
        Task<ReadOnlyCollection<DbColumn>> GetColumnSchemaAsync();
        object GetValue(int ordinal);
    }

    public class DbQueryReader : IDbQueryReader
    {
        private readonly DbDataReader _reader;

        public DbQueryReader(DbDataReader reader) 
        {
            _reader = reader;
        }

        public Task<bool> ReadAsync()
        {
            return _reader.ReadAsync();
        }

        public Task<ReadOnlyCollection<DbColumn>> GetColumnSchemaAsync()
        {
            return _reader.GetColumnSchemaAsync();
        }

        public object GetValue(int ordinal)
        {
            return _reader.GetValue(ordinal);
        }
    }
}
