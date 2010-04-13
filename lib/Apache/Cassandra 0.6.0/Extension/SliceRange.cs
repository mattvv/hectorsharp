using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra060
{
	public partial class SliceRange
	{
		public SliceRange(bool reversed, int count)
			: this(new byte[0], new byte[0], reversed, count)
		{ }

		public SliceRange(byte[] start, byte[] finish)
		{
			if (start != null)
				Start = start;
			else
				Start = new byte[0];

			if (finish != null)
				Finish = finish;
			else
				Finish = new byte[0];

			if(!__isset.reversed)
				reversed = false;
			if(!__isset.count)
				count = 100;
		}

		public SliceRange(string start, string finish)
			: this(start.UTF(), finish.UTF())
		{ }

		public SliceRange(string start, string finish, bool reversed, int count)
			: this(start.UTF(), finish.UTF(), reversed, count)
		{}

		public SliceRange(byte[] start, byte[] finish, bool reversed, int count)
			: this(start, finish)
		{
			Reversed = reversed;
			Count = count;
		}
	}
}
