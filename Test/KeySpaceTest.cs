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
				Assert.Equal("InsertGetRemove.Value." + i, column.Value.DecodeUtf8String());
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
					var value = column.Value.DecodeUtf8String();
					Assert.Equal("BatchInsertColumn.Value." + j, value);
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

		[Fact]
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


		#region IUseFixture<CassandraTestFixture> Members

		CassandraTestFixture fixture;

		public void SetFixture(CassandraTestFixture fixture)
		{
			this.fixture = fixture;
		}

		#endregion
	}

	public class CassandraTestFixture : IDisposable
	{
		internal ICassandraClient Client;
		internal IKeyspace Keyspace;
		internal IKeyedObjectPool<Endpoint, ICassandraClient> Pool;

		public CassandraTestFixture()
		{
			Pool = new CassandraClientPoolFactory().Create();
			Client = new KeyedCassandraClientFactory(Pool, new KeyedCassandraClientFactory.Config { Timeout = 10 })
				.Make(new Endpoint("localhost", 9160));
			Keyspace = Client.GetKeyspace("Keyspace1", ConsistencyLevel.ONE, new FailoverPolicy(0) { Strategy = FailoverStrategy.FAIL_FAST });
		}

		#region IDisposable Members

		public void Dispose()
		{
			if (Client != null)
			{
				Client.MarkAsClosed();
				Client = null;
			}
			if (Pool != null)
			{
				Pool.Close();
				Pool = null;
			}
		}

		#endregion
	}
}
