using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace HectorSharp
{
	static class ToThriftExtension
	{
		public static Apache.Cassandra.ConsistencyLevel ToThrift(this ConsistencyLevel level)
		{
			return (Apache.Cassandra.ConsistencyLevel)level;
		}

		public static Apache.Cassandra.Column ToThrift(this Column c)
		{
			return new Apache.Cassandra.Column
			{
				Name = c.Name.ToUtf8Bytes(),
				Value = c.Value.ToUtf8Bytes(),
				Timestamp = c.Timestamp.Value,
			};
		}

		public static Apache.Cassandra.SuperColumn ToThrift(this SuperColumn sc)
		{
			return new Apache.Cassandra.SuperColumn
			{
				Name = sc.Name.UTF(),
				Columns = sc.Columns.Transform(c => c.ToThrift()).ToList(),
			};
		}

		public static Apache.Cassandra.ColumnPath ToThrift(this ColumnPath path)
		{
			return new Apache.Cassandra.ColumnPath(path.ColumnFamily, path.SuperColumn, path.Column);
		}

		public static Apache.Cassandra.ColumnParent ToThrift(this ColumnParent parent)
		{
			return new Apache.Cassandra.ColumnParent(parent.ColumnFamily, parent.SuperColumn);
		}

		public static Apache.Cassandra.SlicePredicate ToThrift(this SlicePredicate predicate)
		{
			var columnNames = predicate.ColumnNames.Transform(i => i.UTF()).ToList();
			return new Apache.Cassandra.SlicePredicate(columnNames, predicate.SliceRange.ToThrift());
		}

		public static Apache.Cassandra.SliceRange ToThrift(this SliceRange range)
		{
			return new Apache.Cassandra.SliceRange(range.Start.UTF(new byte[0]), range.Finish.UTF(new byte[0]), range.Reversed, range.Count);
		}
	}
}
