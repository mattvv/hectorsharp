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
		CassandraVersion version = CassandraVersion.v0_5_1;

		public class Config
		{
			public int Timeout { get; set; }
			public CassandraVersion CassandraVersion { get; set; }
		}

		public KeyedCassandraClientFactory(IKeyedObjectPool<Endpoint, ICassandraClient> pool, Config config)
		{
			KeyedCassandraClientFactory.monitor = new CassandraClientMonitor();
			this.pool = pool;
			if (config != null)
			{
				this.timeout = config.Timeout;
				this.version = config.CassandraVersion;
			}
		}

		#region IKeyedPoolableObjectFactory<Endpoint,CassandraClient> Members

		public ICassandraClient Make(Endpoint key)
		{
			if (key == null) throw new ArgumentNullException("key");

			var transport = new TSocket(key.Host, key.Port, timeout);
			var protocol = new TBinaryProtocol(transport);

			Apache.Cassandra060.Cassandra.Client v060client = null;
			Apache.Cassandra051.Cassandra.Client v051client = null;

			switch (version)
			{
				case CassandraVersion.v0_6_0:
					v060client = new Apache.Cassandra060.Cassandra.Client(protocol);
					break;

				default:
				case CassandraVersion.v0_5_1:
					v051client = new Apache.Cassandra051.Cassandra.Client(protocol);
					break;
			}
			
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
			
			switch (version)
			{
				case CassandraVersion.v0_6_0:
					return new CassandraClient(v060client, new KeyspaceFactory(monitor), key, pool);

				default:
				case CassandraVersion.v0_5_1:
					return new CassandraClient(v051client, new KeyspaceFactory(monitor), key, pool);
			}

		}

		public void Destroy(Endpoint key, ICassandraClient obj)
		{
			// ((CassandraClientPoolImpl) pool).reportDestroyed(cclient);
			switch (version)
			{
				case CassandraVersion.v0_6_0:
					var v060client = obj.Client as Apache.Cassandra060.Cassandra.Client;
					v060client.InputProtocol.Transport.Close();
					v060client.OutputProtocol.Transport.Close();

					break;
		
				default:
				case CassandraVersion.v0_5_1:
					var v050client = obj.Client as Apache.Cassandra051.Cassandra.Client;
					v050client.InputProtocol.Transport.Close();
					v050client.OutputProtocol.Transport.Close();

					break;
			}
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