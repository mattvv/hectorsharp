using Apache.Cassandra;
using HectorSharp.Utils;
using System.Collections.Generic;
using System;

namespace HectorSharp.Service
{
	static class ToModelExtension
	{
		public static Model.Column ToModel(this Column c)
		{
			return new Model.Column
			{
				Name = c.Name.UTFDecode(),
				Value = c.Value.UTFDecode(),
				Timestamp = c.Timestamp,
			};
		}

		public static Model.SuperColumn ToModel(this SuperColumn sc)
		{
			var columns = new List<Model.Column>(sc.Columns.Transform(item => item.ToModel()));
			return new HectorSharp.Model.SuperColumn(sc.Name.UTFDecode(), columns);
		}
	}
}