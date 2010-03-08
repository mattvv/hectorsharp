using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift;
using HectorSharp.Utils;
using System.Collections.ObjectModel;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Service
{
	class CassandraClientPoolByHost : ObjectPool<CassandraClient>, IObjectPool<CassandraClient>
	{
		public enum WhenExhaustedPolicy { Fail, Grow, Block }
		public static WhenExhaustedPolicy DefaultExhastedPolicy = WhenExhaustedPolicy.Block;
		public static int DefaultMaximumActive = 50;

		// The default max wait time when exhausted happens, default value is negative, which means it'll block indefinitely.
		public static long DefaultMaximumExaustedWaitTime = -1;

		// The default max idle number is 5, so if clients keep idle, the total connection number will decrease to 5
		public static int DefaultMaximumIdleClients = 5;
		//private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolByHostImpl.class);

		CassandraClientFactory clientFactory;
		String url;
		String name;
		int port;
		int maxActive;
		int maxIdle;
		WhenExhaustedPolicy exhaustedPolicy;
		long maxWaitTimeWhenExhausted;
		//GenericObjectPool pool;

		/**
		 * Number of currently blocked threads.
		 * This includes the number of threads waiting for an idle connection, as well as threads
		 * wanting for connection initialization after they won a free slot.
		 */
		Counter blockedThreadsCount = new Counter();

		/**
		 * The set of live clients created by the pool.
		 * This set includes both the active clients currently used by active threads as well as idle
		 * clients waiting in the pool.
		 */
		List<ICassandraClient> liveClientsFromPool;

		public CassandraClientPoolByHost (
			String cassandraUrl, 
			int cassandraPort, 
			String name, 
			ICassandraClientPool pools, 
			CassandraClientMonitor clientMonitor) :
			this(cassandraUrl, cassandraPort, name, pools, clientMonitor, DefaultMaximumActive,
				 DefaultMaximumExaustedWaitTime, DefaultMaximumIdleClients, DefaultExhastedPolicy)
		{}

		public CassandraClientPoolByHost(String cassandraUrl, int cassandraPort, String name, ICassandraClientPool pools, CassandraClientMonitor clientMonitor, int maxActive, long maxWait, int maxIdle, WhenExhaustedPolicy exhaustedPolicy)
		 : this(cassandraUrl, cassandraPort, name, pools, maxActive, maxWait, maxIdle,
				 exhaustedPolicy, new CassandraClientFactory(pools, cassandraUrl, cassandraPort, clientMonitor))
		{}

		public CassandraClientPoolByHost(
			String cassandraUrl, 
			int cassandraPort, 
			String name,
			ICassandraClientPool pools, 
			int maxActive,
			long maxWait, 
			int maxIdle, 
			WhenExhaustedPolicy exhaustedPolicy,
			CassandraClientFactory clientFactory)
		{
			//log.debug("Creating new connection pool for {}:{}", cassandraUrl, cassandraPort);
			url = cassandraUrl;
			port = cassandraPort;
			this.name = name;
			this.maxActive = maxActive;
			this.maxIdle = maxIdle;
			this.maxWaitTimeWhenExhausted = maxWait;
			this.exhaustedPolicy = exhaustedPolicy;
			this.clientFactory = clientFactory;
			// Create a set implemented as a ConcurrentHashMap for performance and concurrency.
			liveClientsFromPool = new List<ICassandraClient>();
			//pool = createPool();
		}

		//Override
		public ICassandraClient borrowClient()
		{
			try
			{
				blockedThreadsCount.Increment();
				ICassandraClient client = (ICassandraClient)pool.borrowObject();
				liveClientsFromPool.Add(client);
				return client;
			}
			catch (Exception e)
			{
				throw new Exception(e.Message);
			}
			finally
			{
				blockedThreadsCount.Decrement();
			}
		}

		//Override
		public void close()
		{
			try
			{
				pool.close();
			}
			catch //(Exception e)
			{
				//log.error("Unable to close pool", e);
			}
		}

		//public int IdleCount { get ; }

		//@Override
		public int getNumIdle()
		{
			return pool.getNumIdle();
		}

		//@Override
		public int getNumActive()
		{
			return pool.getNumActive();
		}

		//@Override
		public int getNumBeforeExhausted()
		{
			return maxActive - pool.getNumActive();
		}

		//@Override
		public void releaseClient(ICassandraClient client)
		{
			pool.returnObject(client);
		}

		private GenericObjectPool createPool()
		{
			//GenericObjectPoolFactory poolFactory = new GenericObjectPoolFactory(clientFactory, maxActive,
			//    getObjectPoolExhaustedAction(exhaustedPolicy),
			//    maxWaitTimeWhenExhausted, maxIdle);
			//return (GenericObjectPool)poolFactory.createPool();
		}

		public static byte getObjectPoolExhaustedAction(WhenExhaustedPolicy exhaustedAction)
		{
			switch (exhaustedAction)
			{
				case WhenExhaustedPolicy.Fail:
					return GenericObjectPool.WHEN_EXHAUSTED_FAIL;
				case WhenExhaustedPolicy.Block:
					return GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
				case WhenExhaustedPolicy.Grow:
					return GenericObjectPool.WHEN_EXHAUSTED_GROW;
				default:
					return GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
			}
		}

		//@Override
		public String toString()
		{
			StringBuilder b = new StringBuilder();
			b.Append("CassandraClientPoolImpl<");
			b.Append(url);
			b.Append(":");
			b.Append(port);
			b.Append(">");
			return b.ToString();
		}

		//Override
		public String getName()
		{
			return name;
		}

		//Override
		public boolean isExhausted()
		{
			return getNumBeforeExhausted() <= 0 &&
				 (exhaustedPolicy.equals(WhenExhaustedPolicy.Block) ||
				  exhaustedPolicy.equals(WhenExhaustedPolicy.Fail));
		}

		//Override
		public int getNumBlockedThreads()
		{
			return blockedThreadsCount.intValue();
		}

		//Override
		public void updateKnownHosts()
		{
			HashSet<ICassandraClient> removed = new HashSet<ICassandraClient>();
			foreach (ICassandraClient c in liveClientsFromPool)
			{
				if (c.isClosed())
				{
					removed.Add(c);
				}
				else
				{
					try
					{
						c.updateKnownHosts();
					}
					catch (Exception e)
					{
						//log.error("Unable to update hosts list at {}", c, e);
						throw e;
					}
				}
			}
			// perform cleanup
			liveClientsFromPool.removeAll(removed);
		}

		//Override
		public List<String> getKnownHosts()
		{
			var hosts = new List<String>();
			foreach (ICassandraClient c in liveClientsFromPool)
			{
				if (!c.isClosed())
				{
					hosts.AddRange(c.getKnownHosts());
				}
			}
			return hosts;
		}

		//Override
		public void invalidateClient(ICassandraClient client)
		{
			try
			{
				liveClientsFromPool.Remove(client);
				client.markAsError();
				pool.invalidateObject(client);
			}
			catch //(Exception e)
			{
				//log.error("Unable to invalidate client " + client, e);
			}
		}

		//Override
		public IList<ICassandraClient> getLiveClients()
		{
			return new ReadOnlyCollection<ICassandraClient>(liveClientsFromPool);
			//return ImmutableSet.copyOf(liveClientsFromPool);
		}

		void reportDestroyed(ICassandraClient client)
		{
			//log.debug("Client has been destroyed: {}", client);
			liveClientsFromPool.Remove(client);
		}

	
#region IObjectPool<CassandraClient> Members

int  IObjectPool<CassandraClient>.Active
{
	get { throw new NotImplementedException(); }
}

int  IObjectPool<CassandraClient>.Idle
{
	get { throw new NotImplementedException(); }
}

CassandraClient  IObjectPool<CassandraClient>.Borrow()
{
 	throw new NotImplementedException();
}

void  IObjectPool<CassandraClient>.Return(CassandraClient obj)
{
 	throw new NotImplementedException();
}

void  IObjectPool<CassandraClient>.Add()
{
 	throw new NotImplementedException();
}

void  IObjectPool<CassandraClient>.Clear()
{
 	throw new NotImplementedException();
}

void  IObjectPool<CassandraClient>.Close()
{
 	throw new NotImplementedException();
}

void  IObjectPool<CassandraClient>.SetFactory(IPoolableObjectFactory<CassandraClient> factory)
{
 	throw new NotImplementedException();
}

#endregion
}
}
