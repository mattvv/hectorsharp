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
			switch (client.Version)
			{
				case CassandraVersion.v0_6_0_beta_3:
					throw new NotImplementedException("Version 0.6.0 not implimented yet");

				default:
				case CassandraVersion.v0_5_1:
					return new _051.Keyspace(
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
}