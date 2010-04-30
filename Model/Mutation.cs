using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp
{
	public class Mutation
	{
		public ColumnOrSuperColumn ColumnOrSuperColumn { get; set; }
		public Deletion Deletion { get; set; }

		public Mutation(ColumnOrSuperColumn corsc)
		{
			ColumnOrSuperColumn = corsc;
		}

		public Mutation(Deletion deletion)
		{
			Deletion = deletion;
		}
	}
}