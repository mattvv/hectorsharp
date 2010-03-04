using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift;
using HectorSharp.Utils;
using System.Collections.ObjectModel;

namespace HectorSharp.Service
{
	/*package*/
	class CassandraClientPoolByHostImpl : CassandraClientPoolByHost
	{
		//private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolByHostImpl.class);

		CassandraClientFactory clientFactory;
		String url;
		String name;
		int port;
		int maxActive;
		int maxIdle;
		ExhaustedPolicy exhaustedPolicy;
		long maxWaitTimeWhenExhausted;
		GenericObjectPool pool;

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

		public CassandraClientPoolByHostImpl (String cassandraUrl, int cassandraPort, String name, ICassandraClientPool pools, CassandraClientMonitor clientMonitor) :
			this(cassandraUrl, cassandraPort, name, pools, clientMonitor, DEFAULT_MAX_ACTIVE,
				 DEFAULT_MAX_WAITTIME_WHEN_EXHAUSTED,
				 DEFAULT_MAX_IDLE, DEFAULT_EXHAUSTED_POLICY)
		{}

		public CassandraClientPoolByHostImpl(String cassandraUrl, int cassandraPort, String name, ICassandraClientPool pools, CassandraClientMonitor clientMonitor, int maxActive, long maxWait, int maxIdle, ExhaustedPolicy exhaustedPolicy)
		 : this(cassandraUrl, cassandraPort, name, pools, maxActive, maxWait, maxIdle,
				 exhaustedPolicy, new CassandraClientFactory(pools, cassandraUrl, cassandraPort, clientMonitor))
		{}

		public CassandraClientPoolByHostImpl(String cassandraUrl, int cassandraPort, String name,
			 ICassandraClientPool pools, int maxActive,
			 long maxWait, int maxIdle, ExhaustedPolicy exhaustedPolicy,
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
			pool = createPool();
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
			GenericObjectPoolFactory poolFactory = new GenericObjectPoolFactory(clientFactory, maxActive,
				 getObjectPoolExhaustedAction(exhaustedPolicy),
				 maxWaitTimeWhenExhausted, maxIdle);
			return (GenericObjectPool)poolFactory.createPool();
		}

		public static byte getObjectPoolExhaustedAction(ExhaustedPolicy exhaustedAction)
		{
			switch (exhaustedAction)
			{
				case WHEN_EXHAUSTED_FAIL:
					return GenericObjectPool.WHEN_EXHAUSTED_FAIL;
				case WHEN_EXHAUSTED_BLOCK:
					return GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
				case WHEN_EXHAUSTED_GROW:
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
				 (exhaustedPolicy.equals(ExhaustedPolicy.WHEN_EXHAUSTED_BLOCK) ||
				  exhaustedPolicy.equals(ExhaustedPolicy.WHEN_EXHAUSTED_FAIL));
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

	}
}
