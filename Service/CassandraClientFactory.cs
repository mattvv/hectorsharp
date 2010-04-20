using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp;
using Thrift.Protocol;
using Thrift.Transport;
using HectorSharp.Utils.ObjectPool;
using HectorSharp.Model;

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
			if (config != null)
			{
				this.timeout = config.Timeout;
			}
		}

		#region IKeyedPoolableObjectFactory<Endpoint,CassandraClient> Members

		public ICassandraClient Make(Endpoint key)
		{
			if (key == null) throw new ArgumentNullException("key");

			var transport = new TSocket(key.Host, key.Port, timeout);
			var protocol = new TBinaryProtocol(transport);

			Apache.Cassandra.Cassandra.Client v060client = null;
			v060client = new Apache.Cassandra.Cassandra.Client(protocol);

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

			return new CassandraClient(v060client, new KeyspaceFactory(monitor), key, pool);
		}

		public void Destroy(Endpoint key, ICassandraClient obj)
		{
			// ((CassandraClientPoolImpl) pool).reportDestroyed(cclient);
			var v060client = obj.Client as Apache.Cassandra.Cassandra.Client;
			v060client.InputProtocol.Transport.Close();
			v060client.OutputProtocol.Transport.Close();

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