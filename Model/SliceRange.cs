using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Model
{
	public class SliceRange
	{
		public string Start { get; set; }
		public string Finish { get; set; }
		public bool Reversed { get; set; }
		public int Count { get; set; }

		public SliceRange()
		{
			Reversed = false;
			Count = 100;
		}

		public SliceRange(bool reversed, int count)
		{
			Reversed = reversed;
			Count = count;
		}

		public SliceRange(string start, string finish, bool reversed, int count)
			: this(reversed, count)
		{
			Start = start;
			Finish = finish;
		}
	}
}
