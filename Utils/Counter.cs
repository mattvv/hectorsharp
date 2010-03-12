using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace HectorSharp.Utils
{
	public class Counter
	{
		long count = 0;

		public Counter()
		{ }

		public Counter(long initialCount)
		{
			count = initialCount;
		}

		public long Increment()
		{
			return Interlocked.Increment(ref count);
		}

		public long Decrement()
		{
			return Interlocked.Decrement(ref count);
		}

		public long Value { get { return count; } }
	}
}