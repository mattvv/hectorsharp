using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using HectorSharp.Utils.ObjectPool;
using HectorSharp.Model;

namespace HectorSharp.Service
{
	internal class KeyspaceFactory
	{
		ICassandraClientMonitor monitor;

		public KeyspaceFactory(ICassandraClientMonitor monitor)
		{
			this.monitor = monitor;
		}

		public IKeyspace Create(
			ICassandraClient client,
			string keyspaceName,
			IDictionary<string, Dictionary<string, string>> keyspaceDesc,
			ConsistencyLevel consistencyLevel,
			FailoverPolicy failoverPolicy,
			IKeyedObjectPool<Endpoint, ICassandraClient> pool)
		{
			return new Keyspace(
				client,
				keyspaceName,
				keyspaceDesc,
				consistencyLevel,
				failoverPolicy,
				pool,
				monitor
				);
		}
	}
}
