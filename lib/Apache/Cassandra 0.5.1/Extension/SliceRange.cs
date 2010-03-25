using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra051
{
	public partial class SliceRange
	{
		public SliceRange(byte[] start, byte[] finish)
		{
			if (start != null && start.Length > 0)
				Start = start;
			if (finish != null && finish.Length > 0)
				Finish = finish;
			if(!__isset.reversed)
				reversed = false;
			if(!__isset.count)
				count = 100;
		}
	
		public SliceRange(byte[] start, byte[] finish, bool reversed, int count)
			: this(start, finish)
		{
			Reversed = reversed;
			Count = count;
		}
	}
}
