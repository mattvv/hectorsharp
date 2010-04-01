using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra051;
using HectorSharp.Utils;

namespace HectorSharp.Service._051
{
	static class ToThriftExtension
	{
		public static Apache.Cassandra051.ConsistencyLevel ToThrift(this ConsistencyLevel level)
		{
			return (Apache.Cassandra051.ConsistencyLevel)level;
		}

		public static Column ToThrift(this Model.Column c)
		{
			return new Column(c.Name.UTF(), c.Value.UTF(new byte[0]), c.Timestamp.Value);
		}

		public static SuperColumn ToThrift(this Model.SuperColumn sc)
		{
			return new SuperColumn
			{
				Name = sc.Name.UTF(),
				Columns = sc.Columns.Transform(c => c.ToThrift()).ToList(),
			};
		}

		public static ColumnPath ToThrift(this Model.ColumnPath path)
		{
			return new ColumnPath(path.ColumnFamily, path.SuperColumn, path.Column);
		}

		public static ColumnParent ToThrift(this Model.ColumnParent parent)
		{
			return new ColumnParent(parent.ColumnFamily, parent.SuperColumn);
		}

		public static SlicePredicate ToThrift(this Model.SlicePredicate predicate)
		{
			var columnNames = predicate.ColumnNames.Transform(i => i.UTF()).ToList();
			return new SlicePredicate(columnNames, predicate.SliceRange.ToThrift());
		}

		public static SliceRange ToThrift(this Model.SliceRange range)
		{
			return new SliceRange(range.Start.UTF(), range.Finish.UTF(), range.Reversed, range.Count);
		}
	}
}
