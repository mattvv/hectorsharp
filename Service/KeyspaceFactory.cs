using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Service
{
	internal class KeyspaceFactory
	{
		//CassandraClientMonitor clientMonitor;

		public KeyspaceFactory(/*CassandraClientMonitor clientMonitor*/)
		{
			//this.clientMonitor = clientMonitor;
		}

		public IKeyspace Create(
			ICassandraClient client,
			string keyspaceName,
			IDictionary<string, IDictionary<string, string>> keyspaceDesc,
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
				pool//, 
				//clientMonitor
				);
		}
	}
}