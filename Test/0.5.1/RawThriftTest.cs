using System;
using System.Collections.Generic;
using Apache.Cassandra051;
using Thrift.Protocol;
using Thrift.Transport;
using Xunit;
using HectorSharp.Utils;

namespace HectorSharp.Test._051
{
	/// <summary>
	/// Tests thrift-generated Cassandra classes and Thrift directly
	/// These are integration tests and require cassandra running locally,
	/// listening to thrift at port 9160
	/// </summary>
	public class RawThriftTest
	{
		/// <summary>
		/// Adapted from sample code at: http://it.toolbox.com/people/joshschulz/journal-entry/4691
		/// </summary>
		[Fact]
		public void SimpleScenario()
		{
			TTransport transport = new TSocket("localhost", 9160);
			TProtocol protocol = new TBinaryProtocol(transport);
			var client = new Cassandra.Client(protocol);

			Console.WriteLine("Opening Connection");
			transport.Open();

			//At this point we're using the standard configuration file
			var nameColumnPath = new ColumnPath("Standard1", null, "name");

			Console.WriteLine("Inserting a column");

			client.insert("Keyspace1",
				"1",
				nameColumnPath,
				"Josh Blogs".UTF(),
				Util.UnixTimestamp,
				ConsistencyLevel.ONE
				);

			client.insert("Keyspace1",
				"2",
				nameColumnPath,
				"Something else".UTF(),
				Util.UnixTimestamp,
				ConsistencyLevel.ONE);

			//Let's get something back out (this is our select statement)
			ColumnOrSuperColumn returnedColumn = client.get(
				"Keyspace1", //The database
				"1", //The actual key we want 
				nameColumnPath, //Where that key sits 
				ConsistencyLevel.ONE //HAZY
				);

			Console.WriteLine("We got Name: {0}, value {1}",
				returnedColumn.Column.Name.UTFDecode(),
				returnedColumn.Column.Value.UTFDecode());

			Console.WriteLine("Now let's try getting a range");

			//This is telling us the offest to get.  This is where paging would occur.
			var predicate = new SlicePredicate(new SliceRange(false, 10));
			var parent = new ColumnParent("Standard1");

			var keyedResults =
				client.multiget_slice("Keyspace1",
					new List<string> { "1", "2" },
					parent,
					predicate,
					ConsistencyLevel.ONE);

			foreach (var keyedResult in keyedResults)
			{
				Console.WriteLine("Key: {0}", keyedResult.Key);
				foreach (ColumnOrSuperColumn result in keyedResult.Value)
				{
					Column column = result.Column;
					Console.WriteLine("Name: {0}, value: {1}",
						column.Name.UTFDecode(),
						column.Value.UTFDecode());
				}
			}

			Console.WriteLine("closing connection");
			transport.Close();
		}

		[Fact]
		public void BlogModelScenario()
		{
			TTransport transport = new TSocket("localhost", 9160);
			TProtocol protocol = new TBinaryProtocol(transport);
			var client = new Cassandra.Client(protocol);

			Console.WriteLine("Opening Connection");
			transport.Open();

			string entryTitle = "now with bonus batch writes";
			string entryAuthor = "josh";
			string entryBody = "This is my blog entry yet again";
			string entryPostDate = DateTime.Now.ToShortDateString();

			var cfmap = new Dictionary<string, List<ColumnOrSuperColumn>>();

			//Column families are case sensitive
			//"BlogEntries"
			cfmap.Add("Standard1", new List<ColumnOrSuperColumn>
			{
				new ColumnOrSuperColumn(new Column("title", entryTitle)),
				new ColumnOrSuperColumn(new Column("body", entryBody)),
				new ColumnOrSuperColumn(new Column("author", entryAuthor)),
				new ColumnOrSuperColumn(new Column("postDate", entryPostDate)),
			});

			client.batch_insert("Keyspace1", entryTitle, cfmap, ConsistencyLevel.ONE);

			//Now Read it back.
			var predicate = new SlicePredicate(new SliceRange(false, 10));
			var parent = new ColumnParent("Standard1"); //"BlogEntries");

			var results = client.get_slice("Keyspace1", entryTitle, parent, predicate, ConsistencyLevel.ONE);

			foreach (ColumnOrSuperColumn resultColumn in results)
			{
				Column column = resultColumn.Column;
				Console.WriteLine("Name: {0}, value: {1}",
					column.Name.UTFDecode(),
					column.Value.UTFDecode());
			}
			Console.WriteLine("closing connection");
			transport.Close();
		}
	}
}