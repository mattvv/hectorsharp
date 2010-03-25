using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Model
{
	public class ColumnParent
	{
		public string ColumnFamily { get; set; }
		public string SuperColumn { get; set; }

		public ColumnParent(string columnFamily)
		{
			ColumnFamily = columnFamily;
		}

		public ColumnParent(string columnFamily, string superColumn)
			: this(columnFamily)
		{
			SuperColumn = superColumn;
		}
	}
}