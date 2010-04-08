using System;
using HectorSharp.Model;
using HectorSharp.Service;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Test._051
{
	public class TestFixture : IDisposable
	{
		internal ICassandraClient Client;
		internal IKeyspace Keyspace;
		internal IKeyedObjectPool<Endpoint, ICassandraClient> Pool;

		public TestFixture()
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
