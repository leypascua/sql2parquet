using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Sql2Parquet
{
    using Parquet.Data;
    using Parquet.Schema;

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
        private readonly Action<List<TElem>, TElem> _addValueDelegate;

        public DataColumnBuilder(int ordinal, DataField field)
        {
            Ordinal = ordinal;
            DataField = field;
            Values = new List<TElem>();

            // Create a delegate for the Add method of List<TElem>
            var addMethod = typeof(List<TElem>).GetMethod("Add", BindingFlags.Public | BindingFlags.Instance);
            _addValueDelegate = (Action<List<TElem>, TElem>)Delegate.CreateDelegate(
                typeof(Action<List<TElem>, TElem>),
                addMethod!);
        }

        public int Ordinal { get; }
        public DataField DataField { get; }
        public List<TElem> Values { get; }
        public int RowCount => Values.Count;

        public void AddValue(object value)
        {
            object finalVal = value == null || value.GetType().IsAssignableFrom(typeof(DBNull)) ?
                null : value;

            _addValueDelegate(Values, (TElem)finalVal);
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
