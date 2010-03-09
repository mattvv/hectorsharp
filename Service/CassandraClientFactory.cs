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
	class KeyedCassandraClientFactory : IKeyedPoolableObjectFactory<Endpoint, CassandraClient>
	{
		CassandraClientMonitor monitor;
		KeyedCassandraClientPool keyedPool;

		int Timeout { get { return 10; } }

		public KeyedCassandraClientFactory(CassandraClientMonitor monitor, KeyedCassandraClientPool keyedPool)
		{
			this.monitor = monitor;
			this.keyedPool = keyedPool;
		}

		#region IKeyedPoolableObjectFactory<Endpoint,CassandraClient> Members

		public CassandraClient Make(Endpoint key)
		{
			if (key == null) throw new ArgumentNullException("key");

			var transport = new TSocket(key.Host, key.Port, Timeout);
			var protocol = new TBinaryProtocol(transport);
			var thriftClient = new Cassandra.Client(protocol);

			try
			{
				transport.Open();
			}
			catch (TTransportException e)
			{
				// Thrift exceptions aren't very good in reporting, so we have to catch the exception here and
				// add details to it.
				throw new Exception("Unable to open transport to " + key.ToString() + " , ", e);
			}

			return new CassandraClient(thriftClient, new KeyspaceFactory(monitor), key, keyedPool.GetPoolByEndpoint(key));
		}

		public void Destroy(Endpoint key, CassandraClient obj)
		{
			// ((CassandraClientPoolImpl) pool).reportDestroyed(cclient);
			var thriftClient = obj.Client;
			thriftClient.InputProtocol.Transport.Close();
			thriftClient.OutputProtocol.Transport.Close();
			obj.markAsClosed();
		}

		public void Activate(Endpoint key, CassandraClient obj)
		{
			/* no-op */
		}

		public bool Passivate(Endpoint key, CassandraClient obj)
		{
			return true;
		}

		public bool Validate(Endpoint key, CassandraClient obj)
		{
			return !obj.IsClosed && !obj.HasErrors;
		}

		#endregion
	}


	class CassandraClientFactory : IPoolableObjectFactory<CassandraClient>
	{
		CassandraClientMonitor monitor;

		IObjectPool<CassandraClient> pool;
		Endpoint endpoint;

		int Timeout { get { return 10; } }

		public CassandraClientFactory(IObjectPool<CassandraClient> pool, Endpoint endpoint, CassandraClientMonitor monitor)
		{
			this.pool = pool;
			this.endpoint = endpoint;
			this.monitor = monitor;
		}

		#region IPoolableObjectFactory<CassandraClient> Members

		public CassandraClient Make()
		{
			TTransport transport = new TSocket(endpoint.Host, endpoint.Port, Timeout);
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
				throw new Exception("Unable to open transport to " + endpoint.ToString() + " , ", e);
			}

			return new CassandraClient(thriftClient,
				 new KeyspaceFactory(monitor), endpoint, pool);
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

		public void Activate(CassandraClient obj)
		{ /* no-op */ }

		public void Passivate(CassandraClient obj)
		{ /* no-op */ }

		public bool Validate(CassandraClient client)
		{
			return !client.IsClosed && !client.HasErrors;
		}

		#endregion
	}
}
