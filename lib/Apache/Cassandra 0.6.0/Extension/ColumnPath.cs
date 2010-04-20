using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra
{
	public partial class ColumnPath
	{
		public ColumnPath(string columnFamily)
		{
			if (!string.IsNullOrEmpty(columnFamily))
				Column_family = columnFamily;
		}

		public ColumnPath(string columnFamily, string superColumn)
			: this(columnFamily, superColumn, null)
		{ }


		public ColumnPath(string columnFamily, string superColumn, string column)
			: this(columnFamily)
		{
			if (!String.IsNullOrEmpty(superColumn))
				Super_column = superColumn.UTF();
			if (!String.IsNullOrEmpty(column))
				Column = column.UTF();
		}
	}
}
