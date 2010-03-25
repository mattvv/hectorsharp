using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Model
{
	public class SuperColumn
	{
		public string Name { get; set; }
		public IList<Column> Columns { get; set; }

		public SuperColumn()
		{}

		public SuperColumn(string name, IList<Column> columns)
		{
			Name = name;
			Columns = columns;
		}
	}
}
