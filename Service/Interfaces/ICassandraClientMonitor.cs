using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Service
{
	public interface ICassandraClientMonitor
	{
		long IncrementCounter(ClientCounter counter);
		/// <summary>
		/// Number of failed (and not-recovered) writes
		/// </summary>
		long WriteFailCount { get; }
		/// <summary>
		/// Number of failed (and not recovered) reads
		/// </summary>
		long ReadFailCount { get; }
		/// <summary>
		/// Number of {@link TimedOutException} that the client has been able to recover from by 
		/// failing over to a different host in the ring.
		/// </summary>
		long RecoverableTimedOutCount { get; }
		/// <summary>
		/// Number of {@link UnavailableException} that the client has been able to recover from by 
		/// failing over to a different host in the ring.
		/// </summary>
		long RecoverableUnavailableCount { get; }
		/// <summary>
		/// Number of {@link TTransportException} that the client has been able to recover from by
		/// failing over to a different host in the ring.
		/// </summary>
		long RecoverableTransportExceptionCount { get; }
		/// <summary>
		/// Total number of recoverable errors which is the sum of RecoverableTimedOutCount, 
		/// RecoverableTimedOutCount and RecoverableTransportExceptionCount.
		/// Returns the total number of recoverable errors by failing over the other hosts.
		/// </summary>
		long RecoverableErrorCount { get; }
		/// <summary>
		/// Number of times a skip-host was performed. Hosts are skipped when there are errors at the current host.
		/// </summary>
		long SkipHostSuccessCount { get; }
		/// <summary>
		/// Number of times clients were requested when connection pools were exhausted.
		/// </summary>
		long NumPoolExhaustedEventCount { get; }
		/// <summary>
		/// Number of existing connection pools. There may be up to one pool per cassandra host.
		/// </summary>
		int PoolCount { get; }
		/// <summary>
		/// endpoints of all exisging pools.
		/// </summary>
		IList<Endpoint> PoolEndpoints { get; }
		/// <summary>
		/// Total number of idle clients in all client pools
		/// </summary>
		int IdleConnectionCount { get; }
		/// <summary>
		/// Total number of active clients in all client pools
		/// </summary>
		int ActiveCount { get; }
		/// <summary>
		/// Number of exhausted connection pools
		/// </summary>
		int ExhaustedPoolCount { get; }
		/// <summary>
		/// Number of recoverable load-balanced connection errors
		/// </summary>
		long RecoverableLoadBalancedConnectErrorCount { get; }
		IList<Endpoint> ExhastedPools { get; }
		/// <summary>
		/// Number of threads that are currently blocked, waiting for a free connection.
		/// This number may be greater than 0 only if the {@link ExhaustedPolicy} is
		/// {@link ExhaustedPolicy#WHEN_EXHAUSTED_BLOCK}
		/// </summary>
		int BlockedThreadCount { get; }
		IList<Endpoint> KnownEndpoints { get; }
		/// <summary>
		/// Tells all pulls to update their list of known hosts.
		/// This is useful when an admin adds/removes a host from the ring and wants the application to
		/// update asap.
		/// </summary>
		void UpdateKnownEndpoints();
	}
}