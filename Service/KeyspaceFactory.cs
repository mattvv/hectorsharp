using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace HectorSharp.Service
{
	/**
	 *
	 * @author Ran Tavory (rantav@gmail.com)
	 *
	 */
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
			ICassandraClientPool clientPools)
		{
			return new KeyspaceImpl(
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
