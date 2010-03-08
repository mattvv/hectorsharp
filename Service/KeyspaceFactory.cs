using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Service
{
	class KeyspaceFactory
	{
		CassandraClientMonitor clientMonitor;

		public KeyspaceFactory(CassandraClientMonitor clientMonitor)
		{
			this.clientMonitor = clientMonitor;
		}

		public IKeyspace create(
			ICassandraClient client,
			String keyspaceName,
			Dictionary<String, Dictionary<String, String>> keyspaceDesc, 
			ConsistencyLevel consistencyLevel,
			FailoverPolicy failoverPolicy,
            IObjectPool<CassandraClient> clientPools)
		{
			return new Keyspace(
				client, 
				keyspaceName, 
				keyspaceDesc, 
				consistencyLevel,
				failoverPolicy, 
				clientPools, 
				clientMonitor);
		}
	}
}