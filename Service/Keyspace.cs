using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Apache.Cassandra;
using HectorSharp.Utils.ObjectPool;
using HectorSharp.Utils;
using Thrift.Transport;

namespace HectorSharp
{
	internal partial class Keyspace
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
		ICassandraClientMonitor monitor;

		public Keyspace(
			ICassandraClient client,
			string keyspaceName,
			IDictionary<string, Dictionary<string, string>> description,
			ConsistencyLevel consistencyLevel,
			FailoverPolicy failoverPolicy,
			IKeyedObjectPool<Endpoint, ICassandraClient> pool,
			ICassandraClientMonitor monitor)
		{
			if (client == null)
				throw new ArgumentNullException("client");

			this.Client = client;
			this.ConsistencyLevel = consistencyLevel;
			this.Description = description;
			this.Name = keyspaceName;
			this.cassandra = client.Client as Cassandra.Client;
			this.FailoverPolicy = failoverPolicy;
			this.pool = pool;
			this.monitor = monitor;
			InitFailover();
		}

		/// <summary>
		/// Make sure that if the given column path was a Column. Throws an InvalidRequestException if not.
		/// </summary>
		/// <param name="columnPath">if either the column family does not exist or that it's type does not match (super)..</param>
		void AssertColumnPath(ColumnPath columnPath)
		//throws InvalidRequestException 
		{
			string cf = columnPath.ColumnFamily;
			IDictionary<string, string> cfdefine;
			if (!Description.ContainsKey(cf))
				throw new InvalidRequestException("The specified column family does not exist: " + cf, null);

			cfdefine = Description[cf];

			if (cfdefine[CF_TYPE].Equals(CF_TYPE_STANDARD) && columnPath.Column != null)
				return; // if the column family is a standard column
			else if (cfdefine[CF_TYPE].Equals(CF_TYPE_SUPER) && columnPath.SuperColumn != null && columnPath.Column != null)
				// if the column family is a super column and also give the super_column name
				return;
		}

		/// <summary>
		///Make sure that the given column path is a SuperColumn in the DB, Throws an exception if it's not.
		/// </summary>
		/// <param name="columnPath"></param>
		void AssertSuperColumnPath(ColumnPath columnPath)
		//throws InvalidRequestException 
		{
			var cf = columnPath.ColumnFamily;
			IDictionary<string, string> cfdefine;

			if ((cfdefine = Description[cf]) != null
				&& cfdefine[CF_TYPE].Equals(CF_TYPE_SUPER)
				 && columnPath.SuperColumn != null)
			{
				return;
			}
			throw new InvalidRequestException(
				 "Invalid super column or super column family does not exist: " + cf, null);
		}

		static List<Apache.Cassandra.ColumnOrSuperColumn> GetSoscList(IEnumerable<Column> columns)
		{
			return new List<Apache.Cassandra.ColumnOrSuperColumn>(columns.Transform(c => new Apache.Cassandra.ColumnOrSuperColumn(c.ToThrift(), null)));
		}

		static List<Apache.Cassandra.ColumnOrSuperColumn> GetSoscSuperList(IEnumerable<SuperColumn> columns)
		{
			return new List<Apache.Cassandra.ColumnOrSuperColumn>(columns.Transform(c => new Apache.Cassandra.ColumnOrSuperColumn(null, c.ToThrift())));
		}

		static List<Column> GetColumnList(IEnumerable<Apache.Cassandra.ColumnOrSuperColumn> columns)
		{
			return new List<Column>(columns.Transform(c => c.Column.ToModel()));
		}

		static List<SuperColumn> GetSuperColumnList(IEnumerable<Apache.Cassandra.ColumnOrSuperColumn> columns)
		{
			return new List<SuperColumn>(columns.Transform(c => c.Super_column.ToModel()));
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
			cassandra = Client.Client as Apache.Cassandra.Cassandra.Client;
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
					}
					catch (Apache.Cassandra.TimedOutException)
					{
						//   log.warn("Got a TimedOutException from {}. Num of retries: {}", client.getUrl(), retries);
						if (retries == 0)
							throw;
						else
						{
							SkipToNextHost();
							monitor.IncrementCounter(ClientCounter.RECOVERABLE_TIMED_OUT_EXCEPTIONS);
						}
					}
					catch (Apache.Cassandra.UnavailableException)
					{
						//  log.warn("Got a UnavailableException from {}. Num of retries: {}", client.getUrl(),
						//      retries);
						if (retries == 0)
							throw;
						else
						{
							SkipToNextHost();
							monitor.IncrementCounter(ClientCounter.RECOVERABLE_UNAVAILABLE_EXCEPTIONS);
						}
					}
					catch (TTransportException)
					{
						//   log.warn("Got a TTransportException from {}. Num of retries: {}", client.getUrl(),
						//       retries);
						if (retries == 0)
							throw;
						else
						{
							SkipToNextHost();
							monitor.IncrementCounter(ClientCounter.RECOVERABLE_TRANSPORT_EXCEPTIONS);
						}
					}
				}
			}
			catch (Apache.Cassandra.InvalidRequestException ex)
			{
				monitor.IncrementCounter(op.FailCounter);
				throw new InvalidRequestException(ex.__isset.why ? ex.Why : ex.Message, ex);
			}
			catch (Apache.Cassandra.UnavailableException ex)
			{
				monitor.IncrementCounter(op.FailCounter);
				throw new UnavailableException(ex.Message, ex);
			}
			/*catch (TException e) {
			monitor.incCounter(op.failCounter);
			throw e;
			} */
			catch (Apache.Cassandra.TimedOutException ex)
			{
				monitor.IncrementCounter(op.FailCounter);
				throw new TimedOutException(ex.Message, ex);
			}
			catch (PoolExhaustedException ex)
			{
				//log.warn("Pool is exhausted", e);
				monitor.IncrementCounter(op.FailCounter);
				monitor.IncrementCounter(ClientCounter.POOL_EXHAUSTED);
				throw new UnavailableException(ex.Message, ex);
			} 
			/*catch (IllegalStateException e) {
			//log.error("Client Pool is already closed, cannot obtain new clients.", e);
			monitor.incCounter(op.failCounter);
			throw new UnavailableException();
			} */
			catch (Exception ex)
			{
				//log.error("Cannot retry failover, got an Exception", e);
				monitor.IncrementCounter(op.FailCounter);
				throw new UnavailableException(ex.Message, ex);
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