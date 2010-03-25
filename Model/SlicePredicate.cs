using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Model
{
	public class SlicePredicate
	{
		public IList<string> ColumnNames { get; set; }
		public SliceRange SliceRange { get; set; }

		public SlicePredicate(IList<string> columnNames, SliceRange sliceRange)
			: this(sliceRange)
		{
			ColumnNames = columnNames;
		}

		public SlicePredicate(SliceRange sliceRange)
		{
			SliceRange = sliceRange;
		}
	}
}
