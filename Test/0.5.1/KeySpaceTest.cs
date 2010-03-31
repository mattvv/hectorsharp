using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Model;
using HectorSharp.Service;
using HectorSharp.Utils;
using HectorSharp.Utils.ObjectPool;
using Moq;
using Thrift.Protocol;
using Thrift.Transport;
using Xunit;

namespace HectorSharp.Test._0_5_1
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
			Assert.NotNull(Keyspace.Description);
			Assert.Equal(4, Keyspace.Description.Count);
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

		[Fact]//(Skip = @"InvalidRequestException : start key's md5 sorts after end key's md5.  \
			//this is not allowed; you probably should not specify end key at all, under RandomPartitioner")]
		public void GetRangeSlice()
		{
			for (int i = 0; i < 10; i++)
			{
				var cp = new ColumnPath("Standard2", null, "GetRangeSlice." + i);
				for (int j = 0; j < 3; j++)
					Keyspace.Insert("GetRangeSlice." + j, cp, "GetRangeSlice.value." + i);
			}

			var columnParent = new ColumnParent("Standard2");
			var predicate = new SlicePredicate(new SliceRange(false, 150));

			var keySlices = Keyspace.GetRangeSlice(columnParent, predicate, "GetRangeSlice.0", null /* "GetRangeSlice.0"*/, 5);

			Assert.NotNull(keySlices);
			Assert.Equal(3, keySlices.Count);
			Assert.NotNull(keySlices["GetRangeSlice.0"]);
			Assert.Equal("GetRangeSlice.value.0", keySlices["GetRangeSlice.0"].First().Value);
			Assert.Equal(10, keySlices["GetRangeSlice.1"].Count);

			var columnPath = new ColumnPath("Standard2");
			for (int i = 0; i < 3; i++)
				Keyspace.Remove("GetRangeSlice." + i, columnPath);
		}

		[Fact(Skip = @"UnavailableException : Internal error processing insert")]
		public void GetSuperRangeSlice()
		{
			for (int i = 0; i < 10; i++)
			{
				var cp = new ColumnPath("Super1", "SuperColumn.1");
				for (int j = 0; j < 2; j++)
					Keyspace.Insert("GetSuperRangeSlice" + j, cp, "GetSuperRangeSlice_value_" + i);
			}
			var columnParent = new ColumnParent("Super1");
			var predicate = new SlicePredicate(null, new SliceRange(false, 150));
			var keySlices = Keyspace.GetSuperRangeSlice(columnParent, predicate, "GetSuperRangeSlice0", "GetSuperRangeSlice3", 5);

			Assert.NotNull(keySlices);
			Assert.Equal(2, keySlices.Count);
			Assert.NotNull(keySlices["GetSuperRangeSlice0"]);
			Assert.Equal("GetSuperRangeSlice_value_0",
				keySlices["GetSuperRangeSlice0"].First().Columns.First().Value);
			Assert.Equal(1, keySlices["GetSuperRangeSlice1"].Count);
			Assert.Equal(10, keySlices["GetSuperRangeSlice1"].First().Columns.Count);

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

		[Fact(Skip="Incomplete")]
		public void Failover()
		{
			var h1client = new Mock<ICassandraClient>();
			var h2client = new Mock<ICassandraClient>();
			var h3client = new Mock<ICassandraClient>();
			var h1endpoint = new Endpoint("h1", 111, "ip1");
			var h2endpoint = new Endpoint("h2", 111, "ip2");
			var h3endpoint = new Endpoint("h3", 111, "ip3");
			var tprotocol = new Mock<TProtocol>(new Mock<TTransport>().Object);
			var h1cassandra = new Mock<Apache.Cassandra051.Cassandra.Client>(tprotocol.Object);
			var h2cassandra = new Mock<Apache.Cassandra051.Cassandra.Client>(tprotocol.Object);
			var h3cassandra = new Mock<Apache.Cassandra051.Cassandra.Client>(tprotocol.Object);
			var keyspaceName = "Keyspace1";
			var description = new Dictionary<string, Dictionary<string, string>>();
			var keyspace1desc = new Dictionary<string, string>();
			keyspace1desc.Add(HectorSharp.Service._051.Keyspace.CF_TYPE, HectorSharp.Service._051.Keyspace.CF_TYPE_STANDARD);
			description.Add("Standard1", keyspace1desc);
			var consistencyLevel = HectorSharp.Service.ConsistencyLevel.ONE;
			var cp = new ColumnPath("Standard1", null, "Failover");
			var clientPool = new Mock<IKeyedObjectPool<Endpoint, ICassandraClient>>();
			var monitor = new Mock<ICassandraClientMonitor>();

			// list of available servers
			var tokenMap = new Dictionary<string, string>();
			tokenMap.Add("t1", "h1");
			tokenMap.Add("t2", "h2");
			tokenMap.Add("t3", "h3");

			h1client.Setup(c => c.Client).Returns(h1cassandra.Object);
			h2client.Setup(c => c.Client).Returns(h2cassandra.Object);
			h3client.Setup(c => c.Client).Returns(h3cassandra.Object);
			h1client.Setup(c => c.GetTokenMap(It.IsAny<bool>())).Returns(tokenMap);
			h2client.Setup(c => c.GetTokenMap(It.IsAny<bool>())).Returns(tokenMap);
			h3client.Setup(c => c.GetTokenMap(It.IsAny<bool>())).Returns(tokenMap);
			h1client.Setup(c => c.Endpoint).Returns(h1endpoint);
			h2client.Setup(c => c.Endpoint).Returns(h2endpoint);
			h3client.Setup(c => c.Endpoint).Returns(h3endpoint);
			clientPool.Setup(p => p.Borrow(It.Is<Endpoint>(e => e == h1endpoint))).Returns(h1client.Object);
			clientPool.Setup(p => p.Borrow(It.Is<Endpoint>(e => e == h2endpoint))).Returns(h2client.Object);
			clientPool.Setup(p => p.Borrow(It.Is<Endpoint>(e => e == h2endpoint))).Returns(h2client.Object);

			// success without failover
			var failoverPolicy = new FailoverPolicy(0) { Strategy = FailoverStrategy.FAIL_FAST };
			var ks = new Service._051.Keyspace(h1client.Object, keyspaceName, description, consistencyLevel, failoverPolicy, clientPool.Object, monitor.Object);

			ks.Insert("key", cp, "value");

			// fail fast

			h1cassandra.Setup(c =>
				c.insert(
					It.IsAny<string>(),
					It.IsAny<string>(),
					It.IsAny<Apache.Cassandra051.ColumnPath>(),
					It.IsAny<byte[]>(),
					It.IsAny<long>(),
					It.IsAny<Apache.Cassandra051.ConsistencyLevel>())
					).Throws(new TimedOutException());

			Assert.Throws<TimedOutException>(() =>
				{
					ks.Insert("key", cp, "value");
				});

			// on fail try next one, h1 fails, h3 succeeds
			failoverPolicy = new FailoverPolicy(3, FailoverStrategy.ON_FAIL_TRY_ONE_NEXT_AVAILABLE);
			ks = new Service._051.Keyspace(h1client.Object, keyspaceName, description, consistencyLevel, failoverPolicy, clientPool.Object, monitor.Object);

			ks.Insert("key", cp, "value");

			h3cassandra.Verify(c =>
				c.insert(
					It.IsAny<string>(),
					It.IsAny<string>(),
					It.IsAny<Apache.Cassandra051.ColumnPath>(),
					It.IsAny<byte[]>(),
					It.IsAny<long>(),
					It.IsAny<Apache.Cassandra051.ConsistencyLevel>())
					);

			clientPool.Verify(p => p.Borrow(h3endpoint));

			// h1 and h3 fail
			ks = new Service._051.Keyspace(h1client.Object, keyspaceName, description, consistencyLevel, failoverPolicy, clientPool.Object, monitor.Object);

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