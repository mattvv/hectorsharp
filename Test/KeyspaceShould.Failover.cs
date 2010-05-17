using System.Collections.Generic;
using HectorSharp.Utils.ObjectPool;
using Moq;
using Thrift.Protocol;
using Thrift.Transport;
using Xunit;
using System.Linq.Expressions;
using System;

namespace HectorSharp.Test
{
	public class KeyspaceFailoverShould : TestBase
	{
		[Fact]
		public void Failover()
		{
			var h1client = new Mock<ICassandraClient>();
			var h2client = new Mock<ICassandraClient>();
			var h3client = new Mock<ICassandraClient>();
			var h1endpoint = new Endpoint("h1", 111, "ip1");
			var h2endpoint = new Endpoint("h2", 111, "ip2");
			var h3endpoint = new Endpoint("h3", 111, "ip3");
			var tprotocol = new Mock<TProtocol>(new Mock<TTransport>().Object);
			var h1cassandra = new Mock<Apache.Cassandra.Cassandra.Client>(tprotocol.Object).As<Apache.Cassandra.Cassandra.Iface>();
			var h2cassandra = new Mock<Apache.Cassandra.Cassandra.Client>(tprotocol.Object).As<Apache.Cassandra.Cassandra.Iface>();
			var h3cassandra = new Mock<Apache.Cassandra.Cassandra.Client>(tprotocol.Object).As<Apache.Cassandra.Cassandra.Iface>();
			var keyspaceName = "Keyspace1";
			var description = new Dictionary<string, Dictionary<string, string>>();
			var keyspace1desc = new Dictionary<string, string>();
			keyspace1desc.Add(HectorSharp.Keyspace.CF_TYPE, HectorSharp.Keyspace.CF_TYPE_STANDARD);
			description.Add("Standard1", keyspace1desc);
			var consistencyLevel = HectorSharp.ConsistencyLevel.ONE;
			var cp = new ColumnPath("Standard1", null, "Failover");
			var pool = new Mock<IKeyedObjectPool<Endpoint, ICassandraClient>>();
			var monitor = new Mock<ICassandraClientMonitor>();

			// list of available servers
			var tokenMap = new Dictionary<string, string>();
			tokenMap.Add("t1", "h1");
			tokenMap.Add("t2", "h2");
			tokenMap.Add("t3", "h3");

			h1client.Setup(c => c.Client).Returns(h1cassandra.Object);
			h2client.Setup(c => c.Client).Returns(h2cassandra.Object);
			h3client.Setup(c => c.Client).Returns(h3cassandra.Object);
			h1client.Setup(c => c.Port).Returns(h1endpoint.Port);
			h2client.Setup(c => c.Port).Returns(h2endpoint.Port);
			h3client.Setup(c => c.Port).Returns(h3endpoint.Port);
			h1client.Setup(c => c.GetTokenMap(AnyBool())).Returns(tokenMap);
			h2client.Setup(c => c.GetTokenMap(AnyBool())).Returns(tokenMap);
			h3client.Setup(c => c.GetTokenMap(AnyBool())).Returns(tokenMap);
			h1client.Setup(c => c.Endpoint).Returns(h1endpoint);
			h2client.Setup(c => c.Endpoint).Returns(h2endpoint);
			h3client.Setup(c => c.Endpoint).Returns(h3endpoint);
			pool.Setup(p => p.Borrow(IsEndpoint(h1endpoint))).Returns(h1client.Object);
			pool.Setup(p => p.Borrow(IsEndpoint(h2endpoint))).Returns(h2client.Object);
			pool.Setup(p => p.Borrow(IsEndpoint(h3endpoint))).Returns(h3client.Object);

			// success without failover

			var failoverPolicy = new FailoverPolicy(0, FailoverStrategy.FAIL_FAST);
			var ks = new Keyspace(h1client.Object, keyspaceName, description, consistencyLevel, failoverPolicy, pool.Object, monitor.Object);

			ks.Insert("key", cp, "value");

			// fail fast

			h1cassandra.Setup(
				c => c.insert(AnyString(), AnyString(), Any<Apache.Cassandra.ColumnPath>(), AnyBytes(), AnyLong(), Any<Apache.Cassandra.ConsistencyLevel>()))
				.Throws(new Apache.Cassandra.TimedOutException());

			Assert.Throws<TimedOutException>(() => ks.Insert("key", cp, "value"));

			// on fail try next one, h1 fails, h2 succeeds
			failoverPolicy = new FailoverPolicy(3, FailoverStrategy.ON_FAIL_TRY_ONE_NEXT_AVAILABLE);
			ks = new Keyspace(h1client.Object, keyspaceName, description, consistencyLevel, failoverPolicy, pool.Object, monitor.Object);

			ks.Insert("key", cp, "value");

			h2cassandra.Verify(
				c => c.insert(AnyString(), AnyString(), Any<Apache.Cassandra.ColumnPath>(), AnyBytes(), AnyLong(), Any<Apache.Cassandra.ConsistencyLevel>())
			);

			pool.Verify(p => p.Borrow(IsEndpoint(h2endpoint)));

			// make all nodes fail

			h2cassandra.Setup(
				c => c.insert(AnyString(), AnyString(), Any<Apache.Cassandra.ColumnPath>(), AnyBytes(), AnyLong(), Any<Apache.Cassandra.ConsistencyLevel>()))
				.Throws(new Apache.Cassandra.TimedOutException());

			h3cassandra.Setup(
				c => c.insert(AnyString(), AnyString(), Any<Apache.Cassandra.ColumnPath>(), AnyBytes(), AnyLong(), Any<Apache.Cassandra.ConsistencyLevel>()))
				.Throws(new Apache.Cassandra.TimedOutException());

			ks = new Keyspace(h1client.Object, keyspaceName, description, consistencyLevel, failoverPolicy, pool.Object, monitor.Object);

			Assert.Throws<TimedOutException>(() => ks.Insert("key", cp, "value"));
		}
	}
}