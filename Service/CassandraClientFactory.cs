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
	public class KeyedCassandraClientFactory : IKeyedPoolableObjectFactory<Endpoint, ICassandraClient>
	{
		static ICassandraClientMonitor monitor;
		IKeyedObjectPool<Endpoint, ICassandraClient> pool;
		Config config;
		int timeout = 10;

		public class Config
		{
			public int Timeout { get; set; }
		}

		public KeyedCassandraClientFactory(IKeyedObjectPool<Endpoint, ICassandraClient> pool, Config config)
		{
			KeyedCassandraClientFactory.monitor = new CassandraClientMonitor();
			this.pool = pool;
			if(config != null)
				this.timeout = config.Timeout;
		}

		#region IKeyedPoolableObjectFactory<Endpoint,CassandraClient> Members

		public ICassandraClient Make(Endpoint key)
		{
			if (key == null) throw new ArgumentNullException("key");

			var transport = new TSocket(key.Host, key.Port, timeout);
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

			return new CassandraClient(thriftClient, new KeyspaceFactory(monitor), key, pool);
		}

		public void Destroy(Endpoint key, ICassandraClient obj)
		{
			// ((CassandraClientPoolImpl) pool).reportDestroyed(cclient);
			var thriftClient = obj.Client;
			thriftClient.InputProtocol.Transport.Close();
			thriftClient.OutputProtocol.Transport.Close();
			obj.MarkAsClosed();
		}

		public void Activate(Endpoint key, ICassandraClient obj)
		{
		}

		public bool Passivate(Endpoint key, ICassandraClient obj)
		{
			return true;
		}

		public bool Validate(Endpoint key, ICassandraClient obj)
		{
			return !obj.IsClosed && !obj.HasErrors;
		}

		#endregion
	}
}