using System;

namespace Apache.Cassandra051
{
	public static class UnixTimestamp
	{
		static readonly DateTime Epoch = new DateTime(1970, 1, 1);

		public static long Current
		{
			get { return Convert.ToInt64((DateTime.UtcNow - Epoch).TotalMilliseconds); }
		}
	}
}