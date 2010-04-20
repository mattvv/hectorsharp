using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra
{
	public partial class Column
	{
		public Column(string name, string value)
			: this(name, value, UnixTimestamp.Current)
		{ }

		public Column(string name, string value, long timestamp)
			: this(name.UTF(), value.UTF(), timestamp)
		{ }

		public Column(byte[] name, byte[] value, long timestamp)
		{
			if(name != null)
				Name = name;
			if (value != null)
				Value = value;
			if (timestamp > 0)
				Timestamp = timestamp;
		}
	}
}
