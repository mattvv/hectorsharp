using Apache.Cassandra;
using HectorSharp.Utils;
using System.Collections.Generic;
using System;

namespace HectorSharp
{
	static class ToModelExtension
	{
		public static Column ToModel(this Apache.Cassandra.Column c)
		{
			return new Column
			{
				Name = c.Name.UTFDecode(),
				Value = c.Value.UTFDecode(),
				Timestamp = c.Timestamp,
			};
		}

		public static SuperColumn ToModel(this Apache.Cassandra.SuperColumn sc)
		{
			var columns = new List<Column>(sc.Columns.Transform(item => item.ToModel()));
			return new HectorSharp.SuperColumn(sc.Name.UTFDecode(), columns);
		}
	}
}