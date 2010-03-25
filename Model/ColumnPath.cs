using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Model
{
	public class ColumnPath
	{
		public string ColumnFamily { get; set; }
		public string SuperColumn { get; set; }
		public string Column { get; set; }

		public ColumnPath(string columnFamily)
		{
			ColumnFamily = columnFamily;
		}

		public ColumnPath(string columnFamily, string superColumn)
			: this(columnFamily)
		{
			SuperColumn = superColumn;
		}

		public ColumnPath(string columnFamily, string superColumn, string column)
			: this(columnFamily, superColumn)
		{
			Column = column;
		}

		public override string ToString()
		{
			return String.Format("ColumnPath(family: '{0}', super: '{1}', column: '{2}'", 
				ColumnFamily, SuperColumn, Column);
		}
	}
}