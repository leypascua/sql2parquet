using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sql2Parquet
{
     using Parquet;
    using Parquet.Data;
    using Parquet.Schema;
    using System.Collections;
    using System.Diagnostics.CodeAnalysis;
    using System.Reflection;

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

        public void AddRow(IDataReader reader)
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

        public interface IDataColumnBuilder
        {
            int Ordinal { get; }
            int RowCount { get; }
            DataField DataField { get; }
            void AddValue(object value);
            DataColumn ToDataColumn();
            void ClearValues();
        }

        public static class DataColumnBuilderFactory
        {
            public static IDataColumnBuilder CreateInstance(int ordinal, DbColumn column)
            {
                var field = new DataField(column.ColumnName, column.DataType, column.AllowDBNull);

                Type columnType = column.DataType switch
                {
                    Type t when t == typeof(string) => typeof(string),
                    _ => field.IsNullable ?
                            typeof(Nullable<>).MakeGenericType(field.ClrType) :
                            field.ClrType
                };

                Type columnBuilderType = typeof(DataColumnBuilder<>).MakeGenericType(columnType);

                return (IDataColumnBuilder)Activator.CreateInstance(columnBuilderType, [ordinal, field]);
            }
        }

        public class DataColumnBuilder<TElem> : IDataColumnBuilder
        {
            public DataColumnBuilder(int ordinal, DataField field)
            {
                Ordinal = ordinal;
                DataField = field;
                Values = new List<TElem>();
                AddValueMethod = typeof(List<TElem>).GetMethod("Add", BindingFlags.Public | BindingFlags.Instance);
            }

            public int Ordinal { get; }
            public DataField DataField { get; }
            public List<TElem> Values { get; }
            public int RowCount => Values.Count;
            public MethodInfo AddValueMethod { get; }

            public void AddValue(object value)
            {
                object finalVal = value == null || value.GetType().IsAssignableFrom(typeof(DBNull)) ?
                    null : value;
                
                AddValueMethod.Invoke(Values, new[] { finalVal });
            }

            public DataColumn ToDataColumn()
            {   
                return new DataColumn(this.DataField, this.Values.ToArray());
            }

            public void ClearValues()
            {
                this.Values.Clear();
            }
        }
    }
}
