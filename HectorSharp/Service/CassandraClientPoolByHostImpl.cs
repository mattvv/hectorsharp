using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift;
using HectorSharp.Utils;

namespace HectorSharp.Service
{
    /*package*/
    class CassandraClientPoolByHostImpl : CassandraClientPoolByHost
    {

        //private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolByHostImpl.class);

        private sealed CassandraClientFactory clientFactory;
        private sealed String url;
        private sealed String name;
        private sealed int port;
        private sealed int maxActive;
        private sealed int maxIdle;
        private sealed ExhaustedPolicy exhaustedPolicy;
        private sealed long maxWaitTimeWhenExhausted;
        private sealed GenericObjectPool pool;

        /**
         * Number of currently blocked threads.
         * This includes the number of threads waiting for an idle connection, as well as threads
         * wanting for connection initialization after they won a free slot.
         */
        private sealed AtomicInteger blockedThreadsCount;

        /**
         * The set of live clients created by the pool.
         * This set includes both the active clients currently used by active threads as well as idle
         * clients waiting in the pool.
         */
        private sealed Set<CassandraClient> liveClientsFromPool;

        public CassandraClientPoolByHostImpl(String cassandraUrl, int cassandraPort, String name, CassandraClientPool pools, CassandraClientMonitor clientMonitor)
        {
            this(cassandraUrl, cassandraPort, name, pools, clientMonitor, DEFAULT_MAX_ACTIVE,
                DEFAULT_MAX_WAITTIME_WHEN_EXHAUSTED,
                DEFAULT_MAX_IDLE, DEFAULT_EXHAUSTED_POLICY);
        }

        public CassandraClientPoolByHostImpl(String cassandraUrl, int cassandraPort, String name, CassandraClientPool pools, CassandraClientMonitor clientMonitor, int maxActive, long maxWait, int maxIdle, ExhaustedPolicy exhaustedPolicy)
        {
            this(cassandraUrl, cassandraPort, name, pools, maxActive, maxWait, maxIdle,
                exhaustedPolicy, new CassandraClientFactory(pools, cassandraUrl, cassandraPort, clientMonitor));

        }

        public CassandraClientPoolByHostImpl(String cassandraUrl, int cassandraPort, String name,
            CassandraClientPool pools, int maxActive,
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
            blockedThreadsCount = new AtomicInteger(0);
            // Create a set implemented as a ConcurrentHashMap for performance and concurrency.
            liveClientsFromPool =
                Collections.newSetFromMap(new ConcurrentDictionary<CassandraClient, Boolean>());
            pool = createPool();
        }

        //Override
        public CassandraClient borrowClient()
        {
            try
            {
                blockedThreadsCount.incrementAndGet();
                CassandraClient client = (CassandraClient)pool.borrowObject();
                liveClientsFromPool.add(client);
                return client;
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
            finally
            {
                blockedThreadsCount.decrementAndGet();
            }
        }

        //Override
        public void close()
        {
            try
            {
                pool.close();
            }
            catch (Exception e)
            {
                //log.error("Unable to close pool", e);
            }
        }

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
        public void releaseClient(CassandraClient client)
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
            HashSet<CassandraClient> removed = new HashSet<CassandraClient>();
            foreach (CassandraClient c in liveClientsFromPool)
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
        public Set<String> getKnownHosts()
        {
            HashSet<String> hosts = new HashSet<String>();
            foreach (CassandraClient c in liveClientsFromPool)
            {
                if (!c.isClosed())
                {
                    hosts.Add(c.getKnownHosts());
                }
            }
            return hosts;
        }

        //Override
        public void invalidateClient(CassandraClient client)
        {
            try
            {
                liveClientsFromPool.remove(client);
                client.markAsError();
                pool.invalidateObject(client);
            }
            catch (Exception e)
            {
                //log.error("Unable to invalidate client " + client, e);
            }
        }

        //Override
        public Set<CassandraClient> getLiveClients()
        {
            return ImmutableSet.copyOf(liveClientsFromPool);
        }

        void reportDestroyed(CassandraClient client)
        {
            //log.debug("Client has been destroyed: {}", client);
            liveClientsFromPool.remove(client);
        }

    }
}
