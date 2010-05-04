using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace HectorSharp.Test
{
	public partial class KeyspaceShould : IUseFixture<HectorSharpFixture>
	{
		ICassandraClient Client { get { return fixture.Client; } }
		IKeyspace Keyspace { get { return fixture.Keyspace; } }

		[Fact]
		public void InsertGetRemove()
		{
			var cp = new ColumnPath("Standard1", null, "InsertGetRemove");

			// insert values
			for (int i = 0; i < 100; i++)
				Keyspace.Insert("InsertGetRemove." + i, cp, "InsertGetRemove.Value." + i);

			// get values
			for (int i = 0; i < 100; i++)
			{
				var column = Keyspace.GetColumn("InsertGetRemove." + i, cp);
				Assert.NotNull(column);
				Assert.Equal("InsertGetRemove.Value." + i, column.Value);
			}

			// remove values
			for (int i = 0; i < 100; i++)
				Keyspace.Remove("InsertGetRemove." + i, cp);

			// make sure they are deleted
			for (int i = 0; i < 100; i++)
				Assert.Throws<NotFoundException>(() =>
				{
					Keyspace.GetColumn("InsertGetRemove." + i, cp);
				});
		}

		[Fact]
		public void ValidateColumnPath()
		{
			var cp = new ColumnPath("Standard1", null, "ValidateColumnPath");
			Keyspace.Insert("ValidateColumnPath", cp, "ValidateColumnPath Value");
			Keyspace.Remove("ValidateColumnPath", cp);

			cp = new ColumnPath("CFDoesNotExist", null, "TestInsertGetRemove");

			Assert.Throws<InvalidRequestException>(() =>
				{
					Keyspace.Insert("ValidateColumnPath", cp, "testValidColumnPath_value");
				});

			cp = new ColumnPath("Standard1", "TestInsertGetRemove", null);

			Assert.Throws<InvalidRequestException>(() =>
				{
					Keyspace.Insert("ValidateColumnPath", cp, "testValidColumnPath_value");
				});
		}

		[Fact]
		public void BatchInsertColumn()
		{
			// insert value
			for (int i = 0; i < 10; i++)
			{
				var columnFamilyMap = new Dictionary<string, IList<Column>>();
				var list = new List<Column>();
				for (int j = 0; j < 10; j++)
					list.Add(new Column("BatchInsertColumn." + j, "BatchInsertColumn.Value." + j));
				columnFamilyMap.Add("Standard1", list);

				Keyspace.BatchInsert("BatchInsertColumn." + i, columnFamilyMap, null);
			}

			// get value
			for (int i = 0; i < 10; i++)
				for (int j = 0; j < 10; j++)
				{
					var columnPath = new ColumnPath("Standard1", null, "BatchInsertColumn." + j);
					var column = Keyspace.GetColumn("BatchInsertColumn." + i, columnPath);
					Assert.NotNull(column);
					Assert.Equal("BatchInsertColumn.Value." + j, column.Value);
				}

			// remove value
			for (int i = 0; i < 10; i++)
				for (int j = 0; j < 10; j++)
				{
					var columnPath = new ColumnPath("Standard1", null, "BatchInsertColumn." + j);
					Keyspace.Remove("BatchInsertColumn." + i, columnPath);
				}
		}

		[Fact]
		public void AssertClient()
		{
			Assert.Equal(Client, Keyspace.Client);
		}

		[Fact]
		public void GetSuperColumn()
		{
			var scmap = new Dictionary<string, IList<SuperColumn>>();
			var list = new List<Column>();
			for (int i = 0; i < 10; i++)
				list.Add(new Column("GetSuperColumn." + i, "GetSuperColumn.Value." + i));

			var superList = new List<SuperColumn> { new SuperColumn("SuperColumn.1", list) };
			scmap.Add("Super1", superList);
			Keyspace.BatchInsert("GetSuperColumn.1", null, scmap);

			var columnPath = new ColumnPath("Super1", "SuperColumn.1", null);

			try
			{
				var superColumn = Keyspace.GetSuperColumn("GetSuperColumn.1", columnPath);
				Assert.NotNull(superColumn);
				Assert.NotNull(superColumn.Columns);
				Assert.Equal(10, superColumn.Columns.Count);
			}
			finally
			{
				Keyspace.Remove("GetSuperColumn.1", columnPath);
			}
		}

		[Fact]
		public void GetSlice()
		{
			// insert
			var columnNames = new List<string>();
			for (int i = 0; i < 100; i++)
			{
				Keyspace.Insert("GetSlice", new ColumnPath("Standard2", null, "GetSlice." + i), "GetSlice.Value." + i);
				columnNames.Add("GetSlice." + i);
			}

			// get
			var columnParent = new ColumnParent("Standard2");
			var sliceRange = new SliceRange(false, 150);
			var slicePredicate = new SlicePredicate(null, sliceRange);
			var columns = Keyspace.GetSlice("GetSlice", columnParent, slicePredicate);

			Assert.NotNull(columns);
			Assert.Equal(100, columns.Count());

			var receivedColumnNames = columns.OrderBy(c => c.Name).Select(c => c.Name).ToList();
			Assert.NotEmpty(receivedColumnNames);
			Assert.Equal(columnNames.OrderBy(i => i).ToList(), receivedColumnNames);

			// clean up
			Keyspace.Remove("GetSlice", new ColumnPath("Standard2"));
		}

		[Fact]
		public void GetSuperSlice()
		{
			// insert
			for (int i = 0; i < 100; i++)
			{
				var cp = new ColumnPath("Super1", "SuperColumn.1", "GetSuperSlice_" + i);
				var cp2 = new ColumnPath("Super1", "SuperColumn.2", "GetSuperSlice_" + i);
				Keyspace.Insert("GetSuperSlice", cp, "GetSuperSlice_value_" + i);
				Keyspace.Insert("GetSuperSlice", cp2, "GetSuperSlice_value_" + i);
			}

			// get
			var columnParent = new ColumnParent("Super1");
			var sliceRange = new SliceRange(false, 150);
			var slicePredicate = new SlicePredicate(null, sliceRange);
			var columns = Keyspace.GetSuperSlice("GetSuperSlice", columnParent, slicePredicate);

			Assert.NotNull(columns);
			Assert.Equal(2, columns.Count());

			// clean up
			Keyspace.Remove("GetSuperSlice", new ColumnPath("Super1"));
		}

		[Fact]
		public void MultigetColumn()
		{
			// insert
			var columnPath = new ColumnPath("Standard1", null, "MultigetColumn");
			var keys = new List<string>();
			for (int i = 0; i < 100; i++)
			{
				var key = "MultigetColumn." + i;
				Keyspace.Insert(key, columnPath, "MultigetColumn.value." + i);
				keys.Add(key);
			}

			// get
			var ms = Keyspace.MultigetColumn(keys, columnPath);
			for (int i = 0; i < 100; i++)
			{
				var column = ms[keys[i]];
				Assert.NotNull(column);
				Assert.Equal("MultigetColumn.value." + i, column.Value);
			}

			// remove
			for (int i = 0; i < 100; i++)
				Keyspace.Remove("MultigetColumn." + i, columnPath);
		}

		[Fact]
		public void MultigetSuperColumn()
		{
			var list = new List<Column>();

			for (int i = 0; i < 10; i++)
				list.Add(new Column("MultigetSuperColumn_" + i, "MultigetSuperColumn_value_" + i));

			var cfmap = new Dictionary<string, IList<SuperColumn>>();
			cfmap.Add("Super1", new List<SuperColumn>() { new SuperColumn("SuperColumn.1", list) });

			Keyspace.BatchInsert("MultigetSuperColumn.1", null, cfmap);

			var columnPath = new ColumnPath("Super1", "SuperColumn.1", null);
			try
			{
				var keys = new List<string> { "MultigetSuperColumn.1" };
				var superColumn = Keyspace.MultigetSuperColumn(keys, columnPath);

				Assert.NotNull(superColumn);
				Assert.Equal(1, superColumn.Count);
				Assert.Equal(10, superColumn["MultigetSuperColumn.1"].Columns.Count);
			}
			finally
			{
				Keyspace.Remove("MultigetSuperColumn.1", columnPath);
			}
		}

		[Fact]
		public void MultigetSlice()
		{
			// insert
			var columnPath = new ColumnPath("Standard1", null, "MultigetSlice");
			var keys = new List<string>();
			for (int i = 0; i < 100; i++)
			{
				var key = "MultigetSlice." + i;
				Keyspace.Insert(key, columnPath, "MultigetSlice.value." + i);
				keys.Add(key);
			}

			// get
			var columnParent = new ColumnParent("Standard1");
			var sliceRange = new SliceRange(false, 150);
			var slicePredicate = new SlicePredicate(null, sliceRange);
			var ms = Keyspace.MultigetSlice(keys, columnParent, slicePredicate);

			for (int i = 0; i < 100; i++)
			{
				var columns = ms[keys[i]];
				Assert.NotNull(columns);
				Assert.Equal(1, columns.Count);
				Assert.True(columns.First().Value.StartsWith("MultigetSlice."));
			}

			// remove
			for (int i = 0; i < 100; i++)
				Keyspace.Remove("MultigetSlice." + i, columnPath);
		}

		[Fact]
		public void MultigetSuperSlice_With_MultigetSlice()
		{
			var list = new List<Column>();
			for (int i = 0; i < 10; i++)
				list.Add(new Column("MultigetSuperSlice." + i, "MultigetSuperSlice.value." + i));

			// super-column map
			var scmap = new Dictionary<string, IList<SuperColumn>>();
			scmap.Add("Super1", new List<SuperColumn>
			{
				new SuperColumn("SuperColumn.1", list),
				new SuperColumn("SuperColumn.2", list),
			});

			var keys = new List<string>();

			for (int i = 1; i <= 3; i++)
			{
				var key = "MultigetSuperSlice." + i;
				Keyspace.BatchInsert(key, null, scmap);
				keys.Add(key);
			}

			try
			{
				var columnParent = new ColumnParent("Super1", "SuperColumn.1");
				var predicate = new SlicePredicate(new SliceRange(false, 150));
				var superc = Keyspace.MultigetSlice(keys, columnParent, predicate);

				Assert.NotNull(superc);
				Assert.Equal(3, superc.Count);
				Assert.NotNull(superc[keys[0]]);
				Assert.Equal(10, superc[keys[0]].Count);
			}
			finally
			{
				var columnPath = new ColumnPath("Super1");
				for (int i = 1; i <= 3; i++)
					Keyspace.Remove("MultigetSuperSlice." + i, columnPath);
			}
		}

		[Fact]
		public void MultigetSuperSlice()
		{
			var scmap = new Dictionary<string, IList<SuperColumn>>();
			var list = new List<Column>();
			for (int i = 0; i < 10; i++)
				list.Add(new Column("MultigetSuperSlice." + i, "MultigetSuperSlice.value." + i));

			scmap.Add("Super1", new List<SuperColumn>
			{
				new SuperColumn("SuperColumn.1", list),
				new SuperColumn("SuperColumn.2", list),
			});

			var keys = new List<string>();

			for (int i = 1; i <= 3; i++)
			{
				var key = "MultigetSuperSlice." + i;
				Keyspace.BatchInsert(key, null, scmap);
				keys.Add(key);
			}

			try
			{
				var columnParent = new ColumnParent("Super1");
				var predicate = new SlicePredicate(new SliceRange(false, 150));
				var superc = Keyspace.MultigetSuperSlice(keys, columnParent, predicate);

				Assert.NotNull(superc);
				Assert.Equal(3, superc.Count);
				var scls = superc[keys[0]];
				Assert.NotNull(scls);
				Assert.Equal(2, scls.Count);
				Assert.NotNull(scls[0].Columns);
				Assert.Equal(10, scls[0].Columns.Count);
				Assert.NotNull(scls[0].Columns[0].Value);
			}
			finally
			{
				var columnPath = new ColumnPath("Super1");
				for (int i = 1; i <= 3; i++)
					Keyspace.Remove("MultigetSuperSlice." + i, columnPath);
			}
		}

		[Fact]
		public void MultigetSuperSlice_With_SuperColumn()
		{
			var list = new List<Column>();
			for (int i = 0; i < 10; i++)
				list.Add(new Column("MultigetSuperSlice." + i, "MultigetSuperSlice.value." + i));

			var scmap = new Dictionary<string, IList<SuperColumn>>();
			scmap.Add("Super1", new List<SuperColumn>
			{
				new SuperColumn("SuperColumn.1", list),
				new SuperColumn("SuperColumn.2", list),
			});

			var keys = new List<string>();

			for (int i = 1; i <= 3; i++)
			{
				var key = "MultigetSuperSlice." + i;
				Keyspace.BatchInsert(key, null, scmap);
				keys.Add(key);
			}

			try
			{
				var columnParent = new ColumnParent("Super1", "SuperColumn.1");
				var predicate = new SlicePredicate(null, new SliceRange(false, 150));
				var superc = Keyspace.MultigetSuperSlice(keys, columnParent, predicate);

				Assert.NotNull(superc);
				Assert.Equal(3, superc.Count);
				var scls = superc[keys[0]];
				Assert.NotNull(scls);
				Assert.Equal(1, scls.Count);
				Assert.NotNull(scls[0].Columns);
				Assert.Equal(10, scls[0].Columns.Count);
				Assert.NotNull(scls[0].Columns[0].Value);
			}
			finally
			{
				var columnPath = new ColumnPath("Super1");
				for (int i = 1; i <= 3; i++)
					Keyspace.Remove("MultigetSuperSlice." + i, columnPath);
			}
		}

		[Fact]
		public void DescribeKeyspace()
		{
			// Included in the default TestCluster config:

			// Standard2
			// Super1
			// Standard1
			// Super2
			// StandardByUUID1

			Assert.NotNull(Keyspace.Description);
			Assert.Equal(5, Keyspace.Description.Count);
		}

		[Fact]
		public void GetCount()
		{
			for (int i = 0; i < 100; i++)
				Keyspace.Insert("GetCount", new ColumnPath("Standard1", null, "GetCount." + i), "GetCount.value." + i);

			int count = Keyspace.GetCount("GetCount", new ColumnParent("Standard1"));
			Assert.Equal(100, count);

			Keyspace.Remove("GetCount", new ColumnPath("Standard1"));
		}

		// get_range_slice is deprecated in 0.6.x, may be removed in 0.7.x
		// use get_range_slices instead
		[Fact]
		public void GetRangeSlice()
		{
			// build 3 keys with 10 columns each
			for (int i = 0; i < 10; i++)
			{
				var cp = new ColumnPath("Standard2", null, "c" + i);
				Keyspace.Insert("rs0", cp, "v" + i);
				Keyspace.Insert("rs1", cp, "v" + i);
				Keyspace.Insert("rs2", cp, "v" + i);
			}

			var columnParent = new ColumnParent("Standard2");
			var predicate = new SlicePredicate(new SliceRange(false, 150));

			var keySlices = Keyspace.GetRangeSlice(columnParent, predicate, "rs0", "rs3", 5);

			Assert.NotNull(keySlices);
			Assert.Equal(3, keySlices.Count);
			Assert.NotNull(keySlices["rs0"]);
			Assert.Equal("v0", keySlices["rs0"].First().Value);
			Assert.Equal(10, keySlices["rs1"].Count);

			var columnPath = new ColumnPath("Standard2");
			for (int i = 0; i < 3; i++)
				Keyspace.Remove("rs" + i, columnPath);
		}

		[Fact]
		public void GetSuperRangeSlice()
		{
			for (int i = 0; i < 10; i++)
			{
				var cp = new ColumnPath("Super1", "SuperColumn.1", "GetSuperRangeSlice." + i);
				Keyspace.Insert("GetSuperRangeSlice.0", cp, "GetSuperRangeSlice_value_" + i);
				Keyspace.Insert("GetSuperRangeSlice.1", cp, "GetSuperRangeSlice_value_" + i);
			}

			var columnParent = new ColumnParent("Super1");
			var predicate = new SlicePredicate(null, new SliceRange(false, 150));
			var keySlices = Keyspace.GetSuperRangeSlice(columnParent, predicate, "GetSuperRangeSlice.0", "GetSuperRangeSlice.3", 5);

			Assert.NotNull(keySlices);

			Assert.Equal(2, keySlices.Where(x => x.Key.StartsWith("GetSuperRangeSlice.")).Count());
			Assert.NotNull(keySlices["GetSuperRangeSlice.0"]);
			Assert.Equal("GetSuperRangeSlice_value_0",
				keySlices["GetSuperRangeSlice.0"].First().Columns.First().Value);
			Assert.Equal(1, keySlices["GetSuperRangeSlice.1"].Count);
			Assert.Equal(10, keySlices["GetSuperRangeSlice.1"].First().Columns.Count);

			var columnPath = new ColumnPath("Super1");

			for (int i = 0; i < 2; i++)
				Keyspace.Remove("GetSuperRangeSlice" + i, columnPath);
		}

		[Fact]
		public void ConsistencyLevel()
		{
			Assert.Equal(1, (int)Keyspace.ConsistencyLevel);
		}

		[Fact]
		public void KeyspaceName()
		{
			Assert.Equal("Keyspace1", Keyspace.Name);
		}

		#region IUseFixture<CassandraTestFixture> Members

		HectorSharpFixture fixture;

		public void SetFixture(HectorSharpFixture fixture)
		{
			this.fixture = fixture;
		}

		#endregion
	}
}