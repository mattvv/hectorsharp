using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp
{
	public class SlicePredicate
	{
		public IList<string> ColumnNames { get; private set; }
		public SliceRange SliceRange { get; set; }

		public SlicePredicate(IList<string> columnNames, SliceRange sliceRange)
			: this(sliceRange)
		{
			if(columnNames.IsNotNullOrEmpty())
				ColumnNames = columnNames;
		}

		public SlicePredicate(SliceRange sliceRange)
			: this()
		{
			SliceRange = sliceRange;
		}

		public SlicePredicate()
		{
			if(ColumnNames == null)
				ColumnNames = new List<String>();
		}
	}
}
