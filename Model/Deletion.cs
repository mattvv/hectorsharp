using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp
{
	public class Deletion
	{
		public int Timestamp { get; set; }
		public string SuperColumn { get; set; }
		public SlicePredicate SlicePredicate { get; set; }
	}
}
