using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using HectorSharp.Utils;
using System.Net;

namespace HectorSharp.Service
{
	/**
	 * Example client that uses the cassandra hector client.
	 *
	 * @author Matt Van Veenendaal (m@mattvv.com)
	 * @author Kris Williams (kris@kris.net)
	 * @author Ran Tavory (rantav@gmail.com) (original java)
	 *
	 */
	public class ExampleClient
	{

		public static void main(String[] args)
		{
			ICassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
			ICassandraClient client = pool.borrowClient("192.168.2.17", 9160);
			try
			{
				IKeyspace keyspace = client.getKeyspace("Keyspace1");
				ColumnPath columnPath = new ColumnPath("Standard1", null, StringUtils.bytes("column-name"));

				// insert
				keyspace.insert("key", columnPath, StringUtils.bytes("value"));

				// read
				Column col = keyspace.getColumn("key", columnPath);

				Console.Out.WriteLine("Read from cassandra: " + col.ToString());

			}
			finally
			{
				// return client to pool. do it in a finally block to make sure it's executed
				pool.releaseClient(client);
			}
		}

		public static void newmain(string[] args)
		{
			// pooling and load-balancing is configured
		   

			using (var cassandra = new CassandraClient("192.168.56.101", 9160))
			{
				var keyspace = cassandra.KeySpaces["keyspace1"];
				var columnPath = new ColumnPath();// ("Test", null, "column-name".ToUtf8Bytes();
				keyspace.Insert("key", columnPath, "value".ToUtf8Bytes());
				var column = keyspace.GetColumn("key", columnPath);
				Console.Out.Write("Read from cassandra: " + column);
			}
		}

		public class HectorConfig
		{
			public int MyProperty { get; set; }
		}
	}
}
