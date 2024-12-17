using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Sql2Parquet
{
    using Parquet;
    using Parquet.Schema;

    public class RowGroupBuilder
    {
        public SortedList<int, IDataColumnBuilder> Data { get; }
        public ParquetSchema Schema { get; }
        public int RowCount => this.Data[0].RowCount;
        public IEnumerable<IDataColumnBuilder> Columns => Data.Values;

        RowGroupBuilder(SortedList<int, IDataColumnBuilder> columns)
        {
            Data = columns;
            Schema = new ParquetSchema(columns.Values.Select(col => col.DataField));
        }

        public static RowGroupBuilder CreateInstance(ReadOnlyCollection<DbColumn> columns)
        {
            var dataColumns = new SortedList<int, IDataColumnBuilder>();

            foreach (var column in columns)
            {
                if (column.DataType == null)
                {
                    throw new InvalidOperationException($"Column '{column.ColumnName}' has no associated DataType.");
                }
                
                int ordinal = column.ColumnOrdinal.GetValueOrDefault(dataColumns.Count);

                dataColumns.Add(ordinal, DataColumnBuilderFactory.CreateInstance(ordinal, column));
            }

            return new RowGroupBuilder(dataColumns);
        }

        public void AddRow(IDbQueryReader reader)
        {
            foreach (var column in this.Data.Values)
            {
                object value = reader.GetValue(column.Ordinal);
                column.AddValue(value);
            }
        }

        public async Task WriteTo(ParquetWriter writer)
        {
            using (var group = writer.CreateRowGroup())
            {
                foreach (var col in Columns)
                {
                    await group.WriteColumnAsync(col.ToDataColumn());
                    col.ClearValues();
                }
            }
        }
    }
}
