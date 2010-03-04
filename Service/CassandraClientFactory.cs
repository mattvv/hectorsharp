using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp;
using Apache.Cassandra;
using Thrift.Protocol;
using Thrift.Transport;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Service
{
	class CassandraClientFactory : IPoolableObjectFactory<CassandraClient>
	{
		CassandraClientMonitor clientMonitor;

		ICassandraClientPool pool;
		String url;
		int port;

		int Timeout { get { return 10; } }

		public CassandraClientFactory(ICassandraClientPool pools, String url, int port, CassandraClientMonitor clientMonitor)
		{
			this.pool = pools;
			this.url = url;
			this.port = port;
			this.clientMonitor = clientMonitor;
		}

		#region IPoolableObjectFactory<CassandraClient> Members

		public CassandraClient Make()
		{
			TTransport transport = new TSocket(url, port, Timeout);
			TProtocol protocol = new TBinaryProtocol(transport);
			Cassandra.Client thriftClient = new Cassandra.Client(protocol);

			try
			{
				transport.Open();
			}
			catch (TTransportException e)
			{
				// Thrift exceptions aren't very good in reporting, so we have to catch the exception here and
				// add details to it.
				throw new Exception("Unable to open transport to " + url + ":" + port + " , ", e);
			}

			return new CassandraClient(thriftClient,
				new KeyspaceFactory(clientMonitor),
				url, port, pool);
		}

		public bool Destroy(CassandraClient client)
		{
			// ((CassandraClientPoolImpl) pool).reportDestroyed(cclient);
			try
			{
				Cassandra.Client thriftClient = client.Client;
				thriftClient.InputProtocol.Transport.Close();
				thriftClient.OutputProtocol.Transport.Close();
				client.markAsClosed();
				return true;
			}
			catch
			{
				return false;
			}
		}

		public bool Activate(CassandraClient obj)
		{
			return true;
		}

		public bool Passivate(CassandraClient obj)
		{
			return true;
		}

		public bool Validate(CassandraClient client)
		{
			return !client.IsClosed && !client.HasErrors;
		}

		#endregion
	}
}
