using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using Thrift;
using System.Net;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Service
{
	/**
	 * We declare this pool as enum to make sure it stays a singlton in the system so clients may
	 * efficiently be reused.
	 *
	 * @author Matt Van Veenendaal (m@mattvv.com)
	 * @author Ran Tavory (ran@outbain.com) [ Original Java version ]
	 *
	 */
	class CassandraClientPool : ObjectPool<CassandraClient>, IObjectPool<CassandraClient>
	//: ICassandraClientPool
	{

		//private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolImpl.class);
		/**
		 * Mapping b/w the host identifier (url:port) and the pool used to store connections to it.
		 */
		Dictionary<PoolKey, CassandraClientPoolByHost> pools;
		CassandraClientMonitor clientMonitor;

		public CassandraClientPool(CassandraClientMonitor clientMonitor)
		{
			pools = new Dictionary<PoolKey, CassandraClientPoolByHost>();
			this.clientMonitor = clientMonitor;
		}

		//Override
		public ICassandraClient borrowClient(String url, int port)
		{
			return getPool(url, port).borrowClient();
		}

		//Override
		public Set<String> getExhaustedPoolNames() {
    Set<String> hosts = new HashSet<String>();
    for (CassandraClientPoolByHost pool: pools.values()) {
      if (pool.isExhausted()) {
        hosts.add(pool.getName());
      }
    }
    return hosts;
  }

		//Override
		public int getNumActive() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      count += pool.getNumActive();
    }
    return count;
  }

		//Override
		public int getNumBlockedThreads() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      count += pool.getNumBlockedThreads();
    }
    return count;
  }

		//Override
		public int getNumExhaustedPools() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      if (pool.isExhausted()) {
        ++count;
      }
    }
    return count;
  }

		//Override
		public int getNumIdle() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      count += pool.getNumIdle();
    }
    return count;
  }

		//Override
		public int getNumPools()
		{
			return pools.size();
		}

		public CassandraClientPoolByHost getPool(String url, int port) {
    PoolKey key = new PoolKey(url, port);
    CassandraClientPoolByHost pool = pools.get(key);
    if (pool == null) {
      synchronized (pools) {
        pool = pools.get(key);
        if (pool == null) {
          pool = new CassandraClientPoolByHostImpl(url, port, key.name, this, clientMonitor);
          pools.put(key, pool);
        }
      }
    }
    return pool;
  }

		//Override
		public Set<String> getPoolNames() {
    Set<String> names = new HashSet<String>();
    for (CassandraClientPoolByHost pool: pools.values()) {
      names.add(pool.getName());
    }
    return names;
  }

		//Override
		public void releaseClient(ICassandraClient client)
		{
			getPool(client).releaseClient(client);
		}

		//Override
		public void updateKnownHosts() {
    for (CassandraClientPoolByHost pool: pools.values()) {
      pool.updateKnownHosts();
    }
  }

		public List<String> getKnownHosts() {
    Set<String> hosts = new HashSet<String>();
    for (CassandraClientPoolByHost pool: pools.values()) {
      hosts.addAll(pool.getKnownHosts());
    }
    return hosts;
  }

		#region PoolKey
		class PoolKey
		{
			String url, ip;
			int port;
			String name;

			public PoolKey(String url, int port)
			{
				this.port = port;
				this.url = url;

				var host = new Uri(url).Host;
				var ip = PoolKey.FindIPAddress(host);

				this.name = string.Format("{0}:{1}", ip == null ? host : ip.ToString(), port);
			}

			static IPAddress FindIPAddress(string host)
			{
				if (String.IsNullOrEmpty(host))
					return null;

				var entry = Dns.Resolve(host);
				var ip = entry.AddressList[0];
				return ip;
			}

			public override string ToString()
			{
				return name;
			}

			public override bool Equals(object obj)
			{
				return ((PoolKey)obj).name.Equals(this.name, StringComparison.InvariantCultureIgnoreCase);
			}

			public override int GetHashCode()
			{
				return base.GetHashCode();
			}
		}

		#endregion

		//Override
		public void invalidateClient(ICassandraClient client)
		{
			getPool(client).invalidateClient(client);

		}

		void reportDestroyed(ICassandraClient client)
		{
			((CassandraClientPoolByHostImpl)getPool(client)).reportDestroyed(client);
		}

		private CassandraClientPoolByHost getPool(ICassandraClient c)
		{
			return getPool(c.getUrl(), c.getPort());
		}

		//Override
		public void releaseKeyspace(IKeyspace k)
		{
			releaseClient(k.getClient());
		}

		//Override
		public ICassandraClient borrowClient(String urlPort)
		{
			int delim = urlPort.LastIndexOf(':');
			String url = urlPort.Substring(0, delim);
			String strPort = urlPort.Substring(delim + 1, urlPort.Length);
			int port = int.Parse(strPort);
			return borrowClient(url, port);
		}

		//Override
		public ICassandraClient borrowClient(String[] clientUrls)
		{
			var clients = new List<String>(clientUrls);
			while (clients.Count > 0)
			{
				Random random = new Random();
				int rand = (int)(random.Next(clients.Count));
				try
				{
					return borrowClient(clients[rand]);
				}
				catch (Exception e)
				{
					if (clients.Count() > 1)
					{
						//log.warn("Unable to obtain client " + clients.get(rand) + " will try the next client", e);
						clientMonitor.incCounter(Counter.RECOVERABLE_LB_CONNECT_ERRORS);
						clients.RemoveAt(rand);
					}
					else
					{
						throw e;
					}
				}
			}
			// Method should never get here; an exception must have been thrown before, I'm only writing
			// this to make the compiler happy.
			return null;
		}

		#region IObjectPool<CassandraClient> Members

		int IObjectPool<CassandraClient>.Active
		{
			get { throw new NotImplementedException(); }
		}

		int IObjectPool<CassandraClient>.Idle
		{
			get { throw new NotImplementedException(); }
		}

		CassandraClient IObjectPool<CassandraClient>.Borrow()
		{
			throw new NotImplementedException();
		}

		void IObjectPool<CassandraClient>.Return(CassandraClient obj)
		{
			throw new NotImplementedException();
		}

		void IObjectPool<CassandraClient>.Add()
		{
			throw new NotImplementedException();
		}

		void IObjectPool<CassandraClient>.Clear()
		{
			throw new NotImplementedException();
		}

		void IObjectPool<CassandraClient>.Close()
		{
			throw new NotImplementedException();
		}

		void IObjectPool<CassandraClient>.SetFactory(IPoolableObjectFactory<CassandraClient> factory)
		{
			throw new NotImplementedException();
		}

		#endregion
	}
}
