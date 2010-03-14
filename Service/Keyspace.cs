using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Apache.Cassandra;
using HectorSharp.Utils.ObjectPool;
using Thrift.Transport;

namespace HectorSharp.Service
{
	partial class Keyspace 
	{
		// constants
		public static string CF_TYPE = "Type";
		public static string CF_TYPE_STANDARD = "Standard";
		public static string CF_TYPE_SUPER = "Super";

		//private static sealed Logger log = LoggerFactory.getLogger(KeyspaceImpl.class);

		Cassandra.Client cassandra; // The cassandra thrift proxy
		/** List of all known remote cassandra nodes */
		List<string> knownHosts = new List<string>();
		IKeyedObjectPool<Endpoint, ICassandraClient> pool;
		CassandraClientMonitor monitor;

		public Keyspace(
			ICassandraClient client,
			string keyspaceName,
			IDictionary<string, IDictionary<string, string>> description,
			ConsistencyLevel consistencyLevel,
			FailoverPolicy failoverPolicy,
			IKeyedObjectPool<Endpoint, ICassandraClient> pool
			/*CassandraClientMonitor monitor*/)
		{
			this.Client = client;
			this.ConsistencyLevel = consistencyLevel;
			this.Description = description;
			this.Name = keyspaceName;
			this.cassandra = client.Client;
			this.FailoverPolicy = failoverPolicy;
			this.pool = pool;
			//this.monitor = monitor;
			InitFailover();
		}

		static readonly DateTime Epoch = new DateTime(1970, 1, 1);
		static long CurrentTimeMillis()
		{
			return (long)(DateTime.UtcNow - Epoch).TotalMilliseconds;
		}

		static long createTimeStamp()
		{
			return CurrentTimeMillis();
		}

		/**
		 * Make sure that if the given column path was a Column. Throws an
		 * InvalidRequestException if not.
		 *
		 * @param columnPath
		 * @throws InvalidRequestException
		 *           if either the column family does not exist or that it's type does
		 *           not match (super)..
		 */
		private void valideColumnPath(ColumnPath columnPath)
		//throws InvalidRequestException 
		{
			string cf = columnPath.Column_family;
			IDictionary<string, string> cfdefine;
			if ((cfdefine = Description[cf]) != null)
			{
				if (cfdefine[CF_TYPE].Equals(CF_TYPE_STANDARD) && columnPath.Column != null)
				{
					// if the column family is a standard column
					return;
				}
				else if (cfdefine[CF_TYPE].Equals(CF_TYPE_SUPER)
					 && columnPath.Super_column != null && columnPath.Column != null)
				{
					// if the column family is a super column and also give the super_column
					// name
					return;
				}
			}
			throw new InvalidRequestException("The specified column family does not exist: " + cf);
		}

		/// <summary>
		///Make sure that the given column path is a SuperColumn in the DB, Throws an exception if it's not.
		/// </summary>
		/// <param name="columnPath"></param>
		void valideSuperColumnPath(ColumnPath columnPath)
		//throws InvalidRequestException 
		{
			var cf = columnPath.Column_family;
			IDictionary<string, string> cfdefine;
			if ((cfdefine = Description[cf]) != null && cfdefine[CF_TYPE].Equals(CF_TYPE_SUPER)
				 && columnPath.Super_column != null)
			{
				return;
			}
			throw new InvalidRequestException(
				 "Invalid super column or super column family does not exist: " + cf);
		}

		static IEnumerable<TOutput> Transform<TInput, TOutput>(
			IEnumerable<TInput> input,
			Func<TInput, TOutput> transform)
		{
			foreach (var item in input) yield return transform(item);
		}

		static List<ColumnOrSuperColumn> GetSoscList(IEnumerable<Column> columns)
		{
			return new List<ColumnOrSuperColumn>(Transform(columns, c => new ColumnOrSuperColumn(c, null)));
		}

		static List<ColumnOrSuperColumn> GetSoscSuperList(IEnumerable<SuperColumn> columns)
		{
			return new List<ColumnOrSuperColumn>(Transform(columns, c => new ColumnOrSuperColumn(null, c)));
		}

		static List<Column> GetColumnList(IEnumerable<ColumnOrSuperColumn> columns)
		{
			return new List<Column>(Transform(columns, c => c.Column));
		}

		static List<SuperColumn> GetSuperColumnList(IEnumerable<ColumnOrSuperColumn> columns)
		{
			return new List<SuperColumn>(Transform(columns, c => c.Super_column));
		}

		/// <summary>
		/// Initializes the ring info so we can handle failover if this happens later.
		/// </summary>
		void InitFailover()
		{
			if (FailoverPolicy.Strategy == FailoverStrategy.FAIL_FAST)
			{
				knownHosts.Clear();
				knownHosts.Add(Client.Endpoint.Host);
				return;
			}
			// learn about other cassandra hosts in the ring
			UpdateKnownHosts();
		}

		/// <summary>
		/// Uses the current known host to query about all other hosts in the ring.
		/// </summary>
		public void UpdateKnownHosts()
		{
			// When update starts we only know of this client, nothing else
			knownHosts.Clear();
			knownHosts.Add(Client.Endpoint.Host);

			// Now query for more hosts. If the query fails, then even this client is now "known"
			try
			{
				var map = Client.GetTokenMap(true);
				knownHosts.Clear();
				foreach (var entry in map)
					knownHosts.Add(entry.Value);
			}
			catch// (TException e) 
			{
				knownHosts.Clear();
				//log.error("Cannot query tokenMap; Keyspace {} is now disconnected", tostring());
			}
		}

		/// <summary>
		/// Updates the client member and cassandra member to the next host in the ring.
		/// Returns the current client to the pool and retreives a new client from the
		/// next pool.
		/// </summary>
		void SkipToNextHost()
		{
			//log.info("Skipping to next host. Current host is: {}", client.getUrl());
			try
			{
				Client.MarkAsError();
				pool.Return(Client.Endpoint, Client);
				Client.RemoveKeyspace(this);
			}
			catch// (Exception e)
			{
				//log.error("Unable to invalidate client {}. Will continue anyhow.", client);
			}

			string nextHost = GetNextHost(Client.Endpoint.Host, Client.Endpoint.IP);
			if (nextHost == null)
			{
				//log.error("Unable to find next host to skip to at {}", tostring());
				throw new Exception("Unable to failover to next host");
			}
			// assume they use the same port
			Client = pool.Borrow(new Endpoint(nextHost, Client.Port));
			cassandra = Client.Client;
			monitor.IncrementCounter(ClientCounter.SKIP_HOST_SUCCESS);
			//log.info("Skipped host. New host is: {}", client.getUrl());
		}

		/// <summary>
		/// Finds the next host in the knownHosts. Next is the one after the given url
		/// (modulo the number of elemens in the list)
		/// </summary>
		/// <param name="url"></param>
		/// <param name="ip"></param>
		/// <returns>URL of the next presumably available host. null if none can be found.</returns>
		string GetNextHost(string url, string ip)
		{
			int size = knownHosts.Count;
			if (size < 1)
				return null;
			for (int i = 0; i < size; ++i)
			{
				if (url.Equals(knownHosts[i]) || ip.Equals(knownHosts[i]))
				{
					// found this host. Return the next one in the array
					return knownHosts[(i + 1) % size];
				}
			}
			// log.error("The URL {} wasn't found in the knownHosts", url);
			return null;
		}

		/// <summary>
		/// Performs the operation and retries in in case the class is configured for
		/// retries, and there are enough hosts to try and the error was 
		/// {@link TimedOutException}.
		/// </summary>
		/// <param name="op"></param>
		void OperateWithFailover(IOperation op)
		{
			int retries = Math.Min((int)FailoverPolicy.RetryCount + 1, knownHosts.Count);
			try
			{
				while (retries > 0)
				{
					--retries;
					// log.debug("Performing operation on {}; retries: {}", client.getUrl(), retries);
					try
					{
						op.Execute(cassandra);
						// hmmm don't count success, there are too many...
						// monitor.incCounter(op.successCounter);
						//   log.debug("Operation succeeded on {}", client.getUrl());
						return;
					}
					catch (TimedOutException e)
					{
						//   log.warn("Got a TimedOutException from {}. Num of retries: {}", client.getUrl(), retries);
						if (retries == 0)
							throw e;
						else
						{
							SkipToNextHost();
							monitor.IncrementCounter(ClientCounter.RECOVERABLE_TIMED_OUT_EXCEPTIONS);
						}
					}
					catch (UnavailableException e)
					{
						//  log.warn("Got a UnavailableException from {}. Num of retries: {}", client.getUrl(),
						//      retries);
						if (retries == 0)
							throw e;
						else
						{
							SkipToNextHost();
							monitor.IncrementCounter(ClientCounter.RECOVERABLE_UNAVAILABLE_EXCEPTIONS);
						}
					}
					catch (TTransportException e)
					{
						//   log.warn("Got a TTransportException from {}. Num of retries: {}", client.getUrl(),
						//       retries);
						if (retries == 0)
							throw e;
						else
						{
							SkipToNextHost();
							monitor.IncrementCounter(ClientCounter.RECOVERABLE_TRANSPORT_EXCEPTIONS);
						}
					}
				}
			}
			catch (InvalidRequestException e)
			{
				monitor.IncrementCounter(op.FailCounter);
				throw e;
			}
			catch (UnavailableException e)
			{
				monitor.IncrementCounter(op.FailCounter);
				throw e;
			} /*catch (TException e) {
      monitor.incCounter(op.failCounter);
      throw e;
    } */
			catch (TimedOutException e)
			{
				monitor.IncrementCounter(op.FailCounter);
				throw e;
			}
			catch (PoolExhaustedException e)
			{
				//log.warn("Pool is exhausted", e);
				monitor.IncrementCounter(op.FailCounter);
				monitor.IncrementCounter(ClientCounter.POOL_EXHAUSTED);
				throw new UnavailableException();
			} /*catch (IllegalStateException e) {
      //log.error("Client Pool is already closed, cannot obtain new clients.", e);
      monitor.incCounter(op.failCounter);
      throw new UnavailableException();
    } */
			catch (Exception)
			{
				//log.error("Cannot retry failover, got an Exception", e);
				monitor.IncrementCounter(op.FailCounter);
				throw new UnavailableException();
			}
		}

		public IList<string> KnownHosts
		{
			get { return new ReadOnlyCollection<string>(knownHosts); }
		}

		public override string ToString()
		{
			return string.Format("Keyspace<{0}>", Client.ToString());
		}
	}
}