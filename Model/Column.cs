using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Utils;

namespace HectorSharp
{
	public class Column
	{
		public string Name { get; set; }
		public string Value { get; set; }
		public long? Timestamp { get; set; }

		public Column()
		{}

		public Column(string name, string value)
			: this(name, value, Util.UnixTimestamp)
		{}

		public Column(string name, string value, long timestamp)
		{
			Name = name;
			Value = value;
			if (timestamp > 0)
				Timestamp = timestamp;
		}

		public override string ToString()
		{
			return string.Format("Column ({0} = {1}){2}", Name, Value, 
				Timestamp.HasValue ? ", [" + Timestamp.Value + "]" : "");
		}
	}
}
