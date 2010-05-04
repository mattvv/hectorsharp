using System.Collections.Generic;
using HectorSharp.Utils.ObjectPool;
using Moq;
using Thrift.Protocol;
using Thrift.Transport;
using Xunit;

namespace HectorSharp.Test
{
	public partial class KeyspaceShould
	{
		[Fact(Skip = "Incomplete")]
		public void Failover()
		{
			var h1client = new Mock<ICassandraClient>();
			var h2client = new Mock<ICassandraClient>();
			var h3client = new Mock<ICassandraClient>();
			var h1endpoint = new Endpoint("h1", 111, "ip1");
			var h2endpoint = new Endpoint("h2", 111, "ip2");
			var h3endpoint = new Endpoint("h3", 111, "ip3");
			var tprotocol = new Mock<TProtocol>(new Mock<TTransport>().Object);
			var h1cassandra = new Mock<Apache.Cassandra.Cassandra.Client>(tprotocol.Object);
			var h2cassandra = new Mock<Apache.Cassandra.Cassandra.Client>(tprotocol.Object);
			var h3cassandra = new Mock<Apache.Cassandra.Cassandra.Client>(tprotocol.Object);
			var keyspaceName = "Keyspace1";
			var description = new Dictionary<string, Dictionary<string, string>>();
			var keyspace1desc = new Dictionary<string, string>();
			keyspace1desc.Add(HectorSharp.Keyspace.CF_TYPE, HectorSharp.Keyspace.CF_TYPE_STANDARD);
			description.Add("Standard1", keyspace1desc);
			var consistencyLevel = HectorSharp.ConsistencyLevel.ONE;
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
			var ks = new Keyspace(h1client.Object, keyspaceName, description, consistencyLevel, failoverPolicy, clientPool.Object, monitor.Object);

			ks.Insert("key", cp, "value");

			// fail fast

			h1cassandra.Setup(c =>
				c.insert(
					It.IsAny<string>(),
					It.IsAny<string>(),
					It.IsAny<Apache.Cassandra.ColumnPath>(),
					It.IsAny<byte[]>(),
					It.IsAny<long>(),
					It.IsAny<Apache.Cassandra.ConsistencyLevel>())
					).Throws(new TimedOutException());

			Assert.Throws<TimedOutException>(() =>
			{
				ks.Insert("key", cp, "value");
			});

			// on fail try next one, h1 fails, h3 succeeds
			failoverPolicy = new FailoverPolicy(3, FailoverStrategy.ON_FAIL_TRY_ONE_NEXT_AVAILABLE);
			ks = new Keyspace(h1client.Object, keyspaceName, description, consistencyLevel, failoverPolicy, clientPool.Object, monitor.Object);

			ks.Insert("key", cp, "value");

			h3cassandra.Verify(c =>
				c.insert(
					It.IsAny<string>(),
					It.IsAny<string>(),
					It.IsAny<Apache.Cassandra.ColumnPath>(),
					It.IsAny<byte[]>(),
					It.IsAny<long>(),
					It.IsAny<Apache.Cassandra.ConsistencyLevel>())
					);

			clientPool.Verify(p => p.Borrow(h3endpoint));

			// h1 and h3 fail
			ks = new Keyspace(h1client.Object, keyspaceName, description, consistencyLevel, failoverPolicy, clientPool.Object, monitor.Object);

		}
	}
}
