using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra051
{
	public partial class ColumnOrSuperColumn
	{
		public ColumnOrSuperColumn(Column column)
			: this(column, null)
		{ }

		public ColumnOrSuperColumn(SuperColumn superColumn)
			: this(null, superColumn)
		{ }

		public ColumnOrSuperColumn(Column column, SuperColumn superColumn)
		{
			if(column != null)
				Column = column;
			if(superColumn != null)
				Super_column = superColumn;
		}
	}
}
