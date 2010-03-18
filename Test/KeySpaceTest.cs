using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Service;
using HectorSharp.Utils.ObjectPool;
using HectorSharp.Utils;
using Xunit;
using Moq;
using Apache.Cassandra;

namespace HectorSharp.Test
{
	public class KeySpaceTest : IUseFixture<CassandraTestFixture>
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
			for(int i = 0; i < 10; i++)
				for (int j = 0; j < 10; j++)
				{
					var columnPath = new ColumnPath("Standard1", null, "BatchInsertColumn." + j);
					var column = Keyspace.GetColumn("BatchInsertColumn." + i, columnPath);
					Assert.NotNull(column);
					Assert.Equal("BatchInsertColumn.Value." + j, column.Value);
				}

			// remove value
			for(int i = 0; i < 10; i++)
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

		[Fact(Skip="haven't solved problem with get_slice")]
		public void GetSuperColumn()
		{
			var columnFamilyMap = new Dictionary<string, IList<SuperColumn>>();
			var list = new List<Column>();
			for (int i = 0; i < 10; i++)
				list.Add(new Column("GetSuperColumn." + i, "GetSuperColumn.Value." + i));

			var superList = new List<SuperColumn> { new SuperColumn("SuperColumn.1", list)};
			columnFamilyMap.Add("Super1", superList);
			Keyspace.BatchInsert("GetSuperColumn.1", null, columnFamilyMap);

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

		[Fact(Skip = "haven't solved problem with get_slice")]
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
			var sliceRange = new SliceRange(new byte[0], new byte[0], false, 150);
			var slicePredicate = new SlicePredicate(null, sliceRange);
			var columns = Keyspace.GetSlice("GetSlice", columnParent, slicePredicate);

			Assert.NotNull(columns);
			Assert.Equal(100, columns.Count());

			var receivedColumnNames = columns.OrderBy(c => c.Name).Select(c => c.Name).ToList();
			Assert.Equal(columnNames, receivedColumnNames);

			// clean up
			Keyspace.Remove("GetSlice", new ColumnPath("Standard2"));
		}

		[Fact(Skip = "haven't solved problem with get_slice")]
		public void GetSuperSlice()
		{
			// insert
			for (int i = 0; i < 100; i++)
			{
				var cp = new ColumnPath("Super1", "SuperColumn_1", "GetSuperSlice_" + i);
				var cp2 = new ColumnPath("Super1", "SuperColumn_2", "GetSuperSlice_" + i);
				Keyspace.Insert("GetSuperSlice", cp, "GetSuperSlice_value_" + i);
				Keyspace.Insert("GetSuperSlice", cp2, "GetSuperSlice_value_" + i);
			}

			// get
			var columnParent = new ColumnParent("Super1");
			var sliceRange = new SliceRange(new byte[0], new byte[0], false, 150);
			var slicePredicate = new SlicePredicate(null, sliceRange);
			var columns = Keyspace.GetSuperSlice("GetSuperSlice", columnParent, slicePredicate);

			Assert.NotNull(columns);
			Assert.Equal(2, columns.Count());

			// clean up
			Keyspace.Remove("GetSuperSlice", new ColumnPath("Super1"));
		}


		#region IUseFixture<CassandraTestFixture> Members

		CassandraTestFixture fixture;

		public void SetFixture(CassandraTestFixture fixture)
		{
			this.fixture = fixture;
		}

		#endregion
	}
}
