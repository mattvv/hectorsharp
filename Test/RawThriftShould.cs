using System;
using System.Collections.Generic;
using Apache.Cassandra;
using Xunit;

namespace Test
{
	/// <summary>
	/// Tests thrift-generated Cassandra classes and Thrift directly
	/// </summary>
	public class RawThriftShould : IUseFixture<RawThriftFixture>
	{
		void Insert(string key, string value, ColumnPath path)
		{
			client.insert("Keyspace1", key, path, value.UTF(), HectorSharp.Util.UnixTimestamp, ConsistencyLevel.ONE); 
		}

		[Fact]
		public void GetRangeSlice()
		{
			env.RestartCassandra();
			env.OpenConnection();

			// build 3 keys with 10 columns each
			for (int i = 0; i < 10; i++)
			{
				var cp = new ColumnPath("Standard2", null, "c" + i);

				Insert("rs0", "v" + i, cp);
				Insert("rs1", "v" + i, cp);
				Insert("rs2", "v" + i, cp);
			}

			var columnParent = new ColumnParent("Standard2");
			var predicate = new SlicePredicate(new SliceRange(false, 150));

			var keySlices = client.get_range_slice(
				"Keyspace1", columnParent, predicate,
				"rs0", "rs3", 5, ConsistencyLevel.ONE);

			Assert.NotNull(keySlices);
			Assert.Equal(3, keySlices.Count);
			Assert.NotNull(keySlices[0]);
			Assert.Equal("v0", keySlices[0].Columns[0].Column.Value.UTFDecode());
			Assert.Equal(10, keySlices[1].Columns.Count);

			env.CloseConnection();
			env.StopCassandra();
		}


		/// <summary>
		/// Adapted from sample code at: http://it.toolbox.com/people/joshschulz/journal-entry/4691
		/// </summary>
		[Fact]
		public void SimpleScenario()
		{
			env.RestartCassandra();
			env.OpenConnection();

			//At this point we're using the standard configuration file
			var cp = new ColumnPath("Standard1", null, "name");

			Console.WriteLine("Inserting a column");

			Insert("1", "Josh Blogs", cp);
			Insert("2", "Something else", cp);

			//Let's get something back out (this is our select statement)
			var returnedColumn = client.get(
				"Keyspace1", //The database
				"1", //The actual key we want 
				cp, //Where that key sits 
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

			env.CloseConnection();
			env.StopCassandra();
		}

		[Fact]
		public void BlogModelScenario()
		{
			env.RestartCassandra();
			env.OpenConnection();

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

			env.CloseConnection();
			env.StopCassandra();
		}


		#region IUseFixture<RawThriftFixture> Members

		RawThriftFixture env;
		Cassandra.Client client;

		public void SetFixture(RawThriftFixture fixture)
		{
			env = fixture;
			client = env.Client;
		}

		#endregion
	}
}