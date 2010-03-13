using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Utils;
using System.Threading;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Service
{


	class CassandraClientMonitor : ICassandraClientMonitor
	{
		//private static final Logger log = LoggerFactory.getLogger(CassandraClientMonitor.class);
		Dictionary<ClientCounter, Counter> counters;
		IKeyedObjectPool<Endpoint, CassandraClient> pool;



		public CassandraClientMonitor()
		{
			// Use a high concurrency map.
			//pool = new KeyedObjectPool<Endpoint, CassandraClient>(factory, config);
			counters = new Dictionary<ClientCounter, Counter>();
			foreach (var counter in Enum.GetValues(typeof(ClientCounter)))
				counters[(ClientCounter)counter] = new Counter();
		}

		public void IncrementCounter(ClientCounter counter)
		{
			counters[counter].Increment();
		}

		public long getWriteSuccess()
		{
			return counters[ClientCounter.WRITE_SUCCESS].Value;
		}

		public long getReadSuccess()
		{
			return counters[ClientCounter.READ_SUCCESS].Value;
		}

		public void updateKnownHosts()
		{
			//log.info("Updating all known cassandra hosts on all clients");
			//foreach (var pool in pool)
			//	pool.updateKnownHosts();
		}

		public List<String> getExhaustedPoolNames()
		{
			var ret = new List<String>();
			//foreach (var pool in pool)
			//   ret.AddRange(pool.getExhaustedPoolNames());
			return ret;
		}

		public int getNumBlockedThreads()
		{
			int ret = 0;
			//foreach (var pool in pool)
			//   ret += pool.getNumBlockedThreads();
			return ret;
		}

		public int getNumExhaustedPools()
		{
			int ret = 0;
			//foreach (var pool in pool)
			//   ret += pool.getNumExhaustedPools();
			return ret;
		}

		public int getNumIdleConnections()
		{
			int ret = 0;
			//foreach (var pool in pool)
			//   ret += pool.getNumIdle();
			return ret;
		}

		public int getNumPools()
		{
			int ret = 0;
			//foreach (var pool in pool)
			//   ret += pool.getNumPools();

			return ret;
		}

		public List<String> getPoolNames()
		{
			var ret = new List<String>();
			foreach (var key in pool.Keys)
				ret.Add(key.ToString());

			return ret;
		}

		public List<String> getKnownHosts()
		{
			var ret = new List<String>();
			//foreach (var pool in pool)
			//   ret.AddRange(pool.getKnownHosts());
			return ret;
		}

		public void AddEndpoint(Endpoint endpoint)
		{
			pool.Add(endpoint);
		}

		public long getRecoverableLoadBalancedConnectErrors()
		{
			return counters[ClientCounter.RECOVERABLE_LB_CONNECT_ERRORS].Value;
		}

		#region ICassandraClientMonitor Members

		public long WriteFailCount
		{
			get { return counters[ClientCounter.WRITE_FAIL].Value; }
		}

		public long ReadFailCount
		{
			get { return counters[ClientCounter.READ_FAIL].Value; }
		}

		public long RecoverableTimedOutCount
		{
			get { return counters[ClientCounter.RECOVERABLE_TIMED_OUT_EXCEPTIONS].Value; }
		}

		public long RecoverableUnavailableCount
		{
			get { return counters[ClientCounter.RECOVERABLE_UNAVAILABLE_EXCEPTIONS].Value; }
		}

		public long RecoverableTransportExceptionCount
		{
			get { return counters[ClientCounter.RECOVERABLE_TRANSPORT_EXCEPTIONS].Value; }
		}

		public long RecoverableErrorCount
		{
			get
			{
				return 
					RecoverableTimedOutCount + 
					RecoverableTransportExceptionCount +
					RecoverableUnavailableCount + 
					RecoverableLoadBalancedConnectErrorCount;
			}
		}

		public long SkipHostSuccessCount
		{
			get { return counters[ClientCounter.SKIP_HOST_SUCCESS].Value; }
		}

		public long NumPoolExhaustedEventCount
		{
			get { return counters[ClientCounter.POOL_EXHAUSTED].Value; }
		}

		public int PoolCount
		{
			get { throw new NotImplementedException(); }
		}

		public IList<Endpoint> PoolEndpoints
		{
			get { throw new NotImplementedException(); }
		}

		public int IdleConnectionCount
		{
			get { return pool.GetIdleCount(); }
		}

		public int ActiveCount
		{
			get { return pool.GetActiveCount(); }
		}

		public int ExhaustedPoolCount
		{
			get { throw new NotImplementedException(); }
		}

		public long RecoverableLoadBalancedConnectErrorCount
		{
			get { throw new NotImplementedException(); }
		}

		public IList<Endpoint> ExhastedPools
		{
			get { throw new NotImplementedException(); }
		}

		public int BlockedThreadCount
		{
			get { throw new NotImplementedException(); }
		}

		public IList<Endpoint> KnownEndpoints
		{
			get { return new List<Endpoint>(pool.Keys); }
		}

		public void UpdateKnownEndpoints()
		{
			throw new NotImplementedException();
		}

		#endregion
	}
}