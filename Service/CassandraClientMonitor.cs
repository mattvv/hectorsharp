using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Utils;
using System.Threading;

namespace HectorSharp.Service
{
	class CassandraClientMonitor : CassandraClientMonitorMBean
	{
		//private static final Logger log = LoggerFactory.getLogger(CassandraClientMonitor.class);
		sealed Dictionary<ClientCounter, Counter> counters;
		sealed List<ICassandraClientPool> pools;

		/// <summary>
		/// List of available JMX counts
		/// </summary>
		public enum ClientCounter
		{
			RECOVERABLE_TIMED_OUT_EXCEPTIONS,
			RECOVERABLE_UNAVAILABLE_EXCEPTIONS,
			RECOVERABLE_TRANSPORT_EXCEPTIONS,
			SKIP_HOST_SUCCESS,
			WRITE_SUCCESS,
			WRITE_FAIL,
			READ_SUCCESS,
			READ_FAIL,
			POOL_EXHAUSTED,
			/** Load balance connection errors */
			RECOVERABLE_LB_CONNECT_ERRORS,
		}

		public CassandraClientMonitor()
		{
			// Use a high concurrency map.
			pools = new List<ICassandraClientPool>();
			counters = new Dictionary<ClientCounter, Counter>();
			foreach (var counter in Enum.GetValues(typeof(ClientCounter)))
				counters[(ClientCounter)counter] = new Counter();
		}

		public void incCounter(ClientCounter counterType)
		{
			counters[counterType].Increment();
		}

		public long getWriteSuccess()
		{
			return counters[ClientCounter.WRITE_SUCCESS].Value;
		}

		public long getReadFail()
		{
			return counters[ClientCounter.READ_FAIL].Value;
		}

		public long getReadSuccess()
		{
			return counters[ClientCounter.READ_SUCCESS].Value;
		}

		public long getSkipHostSuccess()
		{
			return counters[ClientCounter.SKIP_HOST_SUCCESS].Value;
		}

		public long getRecoverableTimedOutCount()
		{
			return counters[ClientCounter.RECOVERABLE_TIMED_OUT_EXCEPTIONS].Value;
		}

		public long getRecoverableUnavailableCount()
		{
			return counters[ClientCounter.RECOVERABLE_UNAVAILABLE_EXCEPTIONS].Value;
		}

		public long getWriteFail()
		{
			return counters[ClientCounter.WRITE_FAIL].Value;
		}

		public void updateKnownHosts()
		{
			//log.info("Updating all known cassandra hosts on all clients");
			foreach (var pool in pools)
				pool.updateKnownHosts();
		}

		public long getNumPoolExhaustedEventCount()
		{
			return counters[ClientCounter.POOL_EXHAUSTED].Value;
		}

		public List<String> getExhaustedPoolNames()
		{
			var ret = new List<String>();
			foreach (var pool in pools)
				ret.AddRange(pool.getExhaustedPoolNames());
			return ret;
		}

		public int getNumActive()
		{
			int ret = 0;
			foreach (var pool in pools)
				ret += pool.getNumActive();
			return ret;
		}


		public int getNumBlockedThreads()
		{
			int ret = 0;
			foreach (var pool in pools)
				ret += pool.getNumBlockedThreads();
			return ret;
		}

		public int getNumExhaustedPools()
		{
			int ret = 0;
			foreach (var pool in pools)
				ret += pool.getNumExhaustedPools();
			return ret;
		}

		public int getNumIdleConnections()
		{
			int ret = 0;
			foreach (var pool in pools)
				ret += pool.getNumIdle();
			return ret;
		}

		public int getNumPools()
		{
			int ret = 0;
			foreach (var pool in pools)
				ret += pool.getNumPools();

			return ret;
		}

		public List<String> getPoolNames()
		{
			var ret = new List<String>();
			foreach (var pool in pools)
				ret.AddRange(pool.getPoolNames());

			return ret;
		}

		public List<String> getKnownHosts()
		{
			var ret = new List<String>();
			foreach (var pool in pools)
				ret.AddRange(pool.getKnownHosts());
			return ret;
		}

		public long getRecoverableTransportExceptionCount()
		{
			return counters[ClientCounter.RECOVERABLE_TRANSPORT_EXCEPTIONS].Value;
		}

		public long getRecoverableErrorCount()
		{
			return getRecoverableTimedOutCount() + getRecoverableTransportExceptionCount() +
				 getRecoverableUnavailableCount() + getRecoverableLoadBalancedConnectErrors();
		}

		public void addPool(ICassandraClientPool pool)
		{
			pools.Add(pool);
		}

		public long getRecoverableLoadBalancedConnectErrors()
		{
			return counters[ClientCounter.RECOVERABLE_LB_CONNECT_ERRORS].Value;
		}
	}
}