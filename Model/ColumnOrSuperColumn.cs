using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp
{
	public class ColumnOrSuperColumn
	{
		public Column Column { get; set; }
		public SuperColumn SuperColumn { get; set; }

		public ColumnOrSuperColumn(Column column, SuperColumn superColumn)
		{
			Column = column;
			SuperColumn = superColumn;
		}
	}
}
