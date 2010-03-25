using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra051
{
	public partial class SlicePredicate
	{
		public SlicePredicate(List<byte[]> column_names, SliceRange slice_range)
		{
			if (column_names != null && column_names.Count > 0)
				Column_names = column_names;
			if (slice_range != null && slice_range.Count > 0)
				Slice_range = slice_range;
		}
	}
}
