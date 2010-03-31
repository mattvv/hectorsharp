using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra060
{
	public partial class ColumnParent
	{
		public ColumnParent(string column_family)
		{
			if(!string.IsNullOrEmpty(column_family))
				Column_family = column_family;
		}

		public ColumnParent(string column_family, string super_column)
			: this(column_family, super_column.UTF())
		{ }

		public ColumnParent(string column_family, byte[] super_column)
			: this(column_family)
		{
			if(super_column != null && super_column.Length > 0)
				Super_column = super_column;
		}
	}
}
