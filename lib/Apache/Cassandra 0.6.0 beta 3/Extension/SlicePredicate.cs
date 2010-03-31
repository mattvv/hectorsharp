using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra060
{
	public partial class SlicePredicate
	{
		public SlicePredicate(SliceRange sliceRange)
		{
			if (sliceRange != null && sliceRange.Count > 0)
				Slice_range = sliceRange;
		}

		public SlicePredicate(List<byte[]> column_names, SliceRange sliceRange)
			: this(sliceRange)
		{
			if (column_names != null && column_names.Count > 0)
				Column_names = column_names;
		}
	}
}
