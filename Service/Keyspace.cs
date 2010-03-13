using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using Thrift;
using Thrift.Transport;
using HectorSharp.Utils.ObjectPool;
using HectorSharp.Utils;
using System.Collections.ObjectModel;

namespace HectorSharp.Service
{
	partial class Keyspace : IKeyspace
	{
		// constants
		public static string CF_TYPE = "Type";
		public static string CF_TYPE_STANDARD = "Standard";
		public static string CF_TYPE_SUPER = "Super";

		//private static sealed Logger log = LoggerFactory.getLogger(KeyspaceImpl.class);

		Cassandra.Client cassandra; // The cassandra thrift proxy
		IDictionary<string, IDictionary<string, string>> description;
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
			this.description = description;
			this.Name = keyspaceName;
			this.cassandra = client.Client;
			this.FailoverPolicy = failoverPolicy;
			this.pool = pool;
			//this.monitor = monitor;
			InitFailover();
		}

		//Override
		public Dictionary<string, Dictionary<string, string>> DescribeKeyspace()
		//throws NotFoundException, TException 
		{
			return description;
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
			Dictionary<string, string> cfdefine;
			if ((cfdefine = description[cf]) != null)
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
			Dictionary<string, string> cfdefine;
			if ((cfdefine = description[cf]) != null && cfdefine[CF_TYPE].Equals(CF_TYPE_SUPER)
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

		interface IOperation
		{
			void Execute(Cassandra.Client client);
			bool HasError { get; }
			NotFoundException Error { get; }
			ClientCounter FailCounter { get; }
		}

		#region Operation<T>

		/// <summary>
		/// Defines the interface of an operation performed on cassandra
		/// </summary>
		/// <typeparam name="T">
		/// The result type of the operation (if it has a result), such as the
		/// result of get_count or get_column
		/// </typeparam>
		class Operation<T> : IOperation
		{
			public Func<Cassandra.Client, T> Handler { get; set; }
			public T Result { get; private set; }
			public bool HasError { get { return Error != null; } }
			public NotFoundException Error { get; set; }
			public ClientCounter FailCounter { get; private set; }

			public Operation(ClientCounter failCounter)
			{
				this.FailCounter = failCounter;
			}

			public Operation(ClientCounter failCounter, Func<Cassandra.Client, T> handler)
				: this(failCounter)
			{
				this.Handler = handler;
			}

			/// <summary>
			/// Performs the operation on the given cassandra instance.
			/// </summary>
			/// <param name="cassandra">client</param>
			/// <returns>null if no execute handler</returns>
			public void Execute(Cassandra.Client cassandra)
			{
				if (Handler != null)
					throw new ApplicationException("Execution Handler was null");

				Result = Handler(cassandra);
			}
		}
		#endregion

		#region VoidOperation
		/// <summary>
		/// Defines the interface of a void operation performed on cassandra
		/// </summary>
		class VoidOperation : IOperation
		{
			public Action<Cassandra.Client> Handler { get; set; }

			public bool HasError { get { return Error != null; } }
			public NotFoundException Error { get; set; }
			public ClientCounter FailCounter { get; private set; }

			public VoidOperation(ClientCounter failCounter)
			{
				this.FailCounter = failCounter;
			}

			public VoidOperation(ClientCounter failCounter, Action<Cassandra.Client> handler)
				: this(failCounter)
			{
				this.Handler = handler;
			}

			/// <summary>
			/// Performs the operation on the given cassandra instance
			/// </summary>
			/// <param name="cassandra">client</param>
			public void Execute(Cassandra.Client client)
			{
				if (Handler != null)
					throw new ApplicationException("Execution Handler was null");

				Handler(client);
			}
		}
		#endregion

		#region IKeyspace Members

		public string Name { get; private set; }
		public ICassandraClient Client { get; private set; }
		public ConsistencyLevel ConsistencyLevel { get; private set; }
		public FailoverPolicy FailoverPolicy { get; private set; }

		public void BatchInsert(string key, IDictionary<string, IEnumerable<Column>> columnMap, IDictionary<string, IEnumerable<SuperColumn>> superColumnMap)
		{
			if (columnMap == null && superColumnMap == null)
				throw new Exception("columnMap and SuperColumnMap can not be null at same time");

			var cfmap = new Dictionary<string, List<ColumnOrSuperColumn>>();

			foreach (var map in columnMap)
				cfmap.Add(map.Key, GetSoscList(map.Value));

			foreach (var map in superColumnMap)
				cfmap.Add(map.Key, GetSoscSuperList(map.Value));

			var op = new VoidOperation(ClientCounter.WRITE_FAIL,
				client => client.batch_insert(Name, key, cfmap, ConsistencyLevel)
			);

			OperateWithFailover(op);
		}

		public IEnumerable<Column> GetSlice(string key, ColumnParent columnParent, SlicePredicate predicate)
		{
			var op = new Operation<IEnumerable<Column>>(ClientCounter.READ_FAIL,
				client =>
				{
					return Transform(
						client.get_slice(Name, key, columnParent, predicate, ConsistencyLevel),
						c => c.Column);
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IEnumerable<SuperColumn> GetSuperSlice(string key, ColumnParent columnParent, SlicePredicate predicate)
		{
			var op = new Operation<IEnumerable<SuperColumn>>(ClientCounter.READ_FAIL,
				client =>
				{
					return Transform(
						client.get_slice(Name, key, columnParent, predicate, ConsistencyLevel),
						c => c.Super_column
					);
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public Column GetColumn(string key, ColumnPath columnPath)
		{
			valideColumnPath(columnPath);

			var op = new Operation<Column>(ClientCounter.READ_FAIL);
			op.Handler = client =>
			{
				try
				{
					var cosc = client.get(Name, key, columnPath, ConsistencyLevel);
					return cosc == null ? null : cosc.Column;
				}
				catch (NotFoundException ex)
				{
					op.Error = ex;
				}
				return null;
			};

			OperateWithFailover(op);

			if (op.HasError)
				throw op.Error;
			return op.Result;
		}

		public int GetCount(string key, ColumnParent columnParent)
		{
			var op = new Operation<int>(ClientCounter.READ_FAIL,
				client =>
				{
					return client.get_count(Name, key, columnParent, ConsistencyLevel);
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IDictionary<string, Column> MultigetColumn(IList<string> keys, ColumnPath columnPath)
		{
			valideColumnPath(columnPath);

			var op = new Operation<IDictionary<string, Column>>(ClientCounter.READ_FAIL,
				client =>
				{
					var result = new Dictionary<string, Column>();
					var cfmap = client.multiget(Name, keys, columnPath, ConsistencyLevel);

					foreach (var entry in Transform(cfmap, entry => new { entry.Key, entry.Value.Column }))
						result.Add(entry.Key, entry.Column);

					return result;
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IDictionary<string, SuperColumn> MultigetSuperColumn(IList<string> keys, ColumnPath columnPath)
		{
			throw new NotImplementedException();
		}

		public IDictionary<string, SuperColumn> MultigetSuperColumn(IList<string> keys, ColumnPath columnPath, bool reversed, int size)
		{
			throw new NotImplementedException();
		}

		public IDictionary<string, IList<Column>> MultigetSlice(IList<string> keys, ColumnParent columnParent, SlicePredicate predicate)
		{
			throw new NotImplementedException();
		}

		public IDictionary<string, IList<SuperColumn>> MultigetSuperSlice(IList<string> keys, ColumnParent columnParent, SlicePredicate predicate)
		{
			var op = new Operation<IDictionary<string, IList<SuperColumn>>>(ClientCounter.READ_FAIL,
				client =>
				{
					var cfmap = client.multiget_slice(Name, keys, columnParent, predicate, ConsistencyLevel);

				}
			);
			OperateWithFailover(op);
			return op.Result;
		}

		public Dictionary<string, List<SuperColumn>> MultigetSuperSlice(List<string> keys,
	 ColumnParent columnParent, SlicePredicate predicate)
		//throws InvalidRequestException, UnavailableException, TException, TimedOutException 
		{
			//Operation<Map<string, List<SuperColumn>>> getCount = new Operation<Map<string, List<SuperColumn>>>(
			//    Counter.READ_FAIL) {
			//  @Override
			//  public Map<string, List<SuperColumn>> execute(Client cassandra)
			//      throws InvalidRequestException, UnavailableException, TException, TimedOutException {
			//    Map<string, List<ColumnOrSuperColumn>> cfmap = cassandra.multiget_slice(keyspaceName, keys,
			//        columnParent, predicate, consistency);

			//    // if user not given super column name, the multiget_slice will return
			//    // List
			//    // filled with
			//    // super column, if user given a column name, the return List will
			//    // filled
			//    // with column,
			//    // this is a bad interface design.
			//    if (columnParent.getSuper_column() == null) {
			//      Map<string, List<SuperColumn>> result = new HashMap<string, List<SuperColumn>>();
			//      for (Map.Entry<string, List<ColumnOrSuperColumn>> entry : cfmap.entrySet()) {
			//        result.put(entry.getKey(), getSuperColumnList(entry.getValue()));
			//      }
			//      return result;
			//    } else {
			//      Map<string, List<SuperColumn>> result = new HashMap<string, List<SuperColumn>>();
			//      for (Map.Entry<string, List<ColumnOrSuperColumn>> entry : cfmap.entrySet()) {
			//        SuperColumn spc = new SuperColumn(columnParent.getSuper_column(),
			//            getColumnList(entry.getValue()));
			//        ArrayList<SuperColumn> spclist = new ArrayList<SuperColumn>(1);
			//        spclist.add(spc);
			//        result.put(entry.getKey(), spclist);
			//      }
			//      return result;
			//    }
			//  }
			//};
			//operateWithFailover(getCount);
			//return getCount.getResult();
			return new Dictionary<string, List<SuperColumn>>();
		}


		public IDictionary<string, IDictionary<string, string>> DescribeKeyspace()
		{
			throw new NotImplementedException();
		}

		public IDictionary<string, IList<Column>> GetRangeSlice(ColumnParent columnParent, SlicePredicate predicate, string start, string finish, int count)
		{
			var op = new Operation<IDictionary<string, IList<Column>>>(ClientCounter.READ_FAIL,
				client =>
				{
					var result = new Dictionary<string, IList<Column>>();
					var keySlices = client.get_range_slice(Name, columnParent, predicate, start, finish, count, ConsistencyLevel);
					if (keySlices == null || keySlices.Count == 0)
						return result;

					foreach (var entry in Transform(keySlices, entry => new { entry.Key, Columns = GetColumnList(entry.Columns) }))
						result.Add(entry.Key, entry.Columns);

					return result;
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IDictionary<string, IList<SuperColumn>> GetSuperRangeSlice(ColumnParent columnParent, SlicePredicate predicate, string start, string finish, int count)
		{
			var op = new Operation<IDictionary<string, IList<SuperColumn>>>(ClientCounter.READ_FAIL,
				client =>
				{
					var result = new Dictionary<string, IList<SuperColumn>>();
					var keySlices = client.get_range_slice(Name, columnParent, predicate, start, finish, count, ConsistencyLevel);
					if (keySlices == null || keySlices.Count == 0)
						return result;

					foreach (var entry in Transform(keySlices, entry => new { entry.Key, Columns = GetSuperColumnList(entry.Columns) }))
						result.Add(entry.Key, entry.Columns);

					return result;
				}
			);
			OperateWithFailover(op);
			return op.Result;
		}

		//Override
		public SuperColumn GetSuperColumn(string key, ColumnPath columnPath)
		//throws InvalidRequestException, NotFoundException, UnavailableException, TException,
		//TimedOutException 
		{
			return GetSuperColumn(key, columnPath, false, Int32.MaxValue);
		}

		//Override
		public SuperColumn GetSuperColumn(string key, ColumnPath columnPath,
			 bool reversed, int size)
		//throws InvalidRequestException, NotFoundException,
		//UnavailableException, TException, TimedOutException 
		{
			//valideSuperColumnPath(columnPath);
			SliceRange sliceRange = new SliceRange(new byte[0], new byte[0], reversed, size);

			//Operation<SuperColumn> op = new Operation<SuperColumn>(Counter.READ_FAIL) {
			//  @Override
			//  public SuperColumn execute(Client cassandra) throws InvalidRequestException,
			//      UnavailableException, TException, TimedOutException {
			//    ColumnParent clp = new ColumnParent(columnPath.getColumn_family(),
			//        columnPath.getSuper_column());
			//    SlicePredicate sp = new SlicePredicate(null, sliceRange);
			//    List<ColumnOrSuperColumn> cosc = cassandra.get_slice(keyspaceName, key, clp, sp,
			//        consistency);
			//    return new SuperColumn(columnPath.getSuper_column(), getColumnList(cosc));
			//  }
			//};
			//operateWithFailover(op);
			//return op.getResult();
			return new SuperColumn();
		}


		//Override
		public void Insert(string key, ColumnPath columnPath, byte[] value)
		//   throws InvalidRequestException, UnavailableException, TException, TimedOutException
		{
			//valideColumnPath(columnPath);
			//Operation<Void> op = new Operation<Void>(Counter.WRITE_FAIL) {
			//  @Override
			//  public Void execute(Client cassandra) throws InvalidRequestException, UnavailableException,
			//      TException, TimedOutException {
			//    cassandra.insert(keyspaceName, key, columnPath, value, createTimeStamp(), consistency);
			//    return null;
			//  }
			//};
			//operateWithFailover(op);
		}



		//Override
		public Dictionary<string, List<Column>> MultigetSlice(List<string> keys,
			  ColumnParent columnParent, SlicePredicate predicate)
		// throws InvalidRequestException, UnavailableException, TException, TimedOutException 
		{
			//Operation<Map<string, List<Column>>> getCount = new Operation<Map<string, List<Column>>>(
			//    Counter.READ_FAIL) {
			//  @Override
			//  public Map<string, List<Column>> execute(Client cassandra) throws InvalidRequestException,
			//      UnavailableException, TException, TimedOutException {
			//    Map<string, List<ColumnOrSuperColumn>> cfmap = cassandra.multiget_slice(keyspaceName, keys,
			//        columnParent, predicate, consistency);

			//    Map<string, List<Column>> result = new HashMap<string, List<Column>>();
			//    for (Map.Entry<string, List<ColumnOrSuperColumn>> entry : cfmap.entrySet()) {
			//      result.put(entry.getKey(), getColumnList(entry.getValue()));
			//    }
			//    return result;
			//  }
			//};
			//operateWithFailover(getCount);
			//return getCount.getResult();
			return new Dictionary<string, List<Column>>();

		}

		//Override
		public Dictionary<string, SuperColumn> MultigetSuperColumn(List<string> keys, ColumnPath columnPath)
		//throws InvalidRequestException, UnavailableException, TException, TimedOutException 
		{
			return MultigetSuperColumn(keys, columnPath, false, Int32.MaxValue);
		}

		//Override
		public Dictionary<string, SuperColumn> MultigetSuperColumn(List<string> keys, ColumnPath columnPath,
			 bool reversed, int size)
		///throws InvalidRequestException, UnavailableException, TException, TimedOutException 
		{
			//valideSuperColumnPath(columnPath);

			// only can get supercolumn by multigetSuperSlice
			ColumnParent clp = new ColumnParent(columnPath.Column_family, columnPath.Super_column);
			SliceRange sr = new SliceRange(new byte[0], new byte[0], reversed, size);
			SlicePredicate sp = new SlicePredicate(null, sr);
			var sclist = MultigetSuperSlice(keys, clp, sp);

			if (sclist == null || sclist.Count == 0)
			{
				return new Dictionary<string, SuperColumn>();
			}

			var result = new Dictionary<string, SuperColumn>();
			foreach (var entry in sclist)
			{
				var sclistByKey = entry.Value;
				if (sclistByKey.Count > 0)
					result.Add(entry.Key, sclistByKey[0]);
			}
			return result;
		}

		public void Remove(string key, ColumnPath columnPath)
		{
			var op = new VoidOperation(ClientCounter.WRITE_FAIL,
				client =>
				{
					client.remove(Name, key, columnPath, createTimeStamp(), ConsistencyLevel);
				}
			);
			OperateWithFailover(op);
		}

		#endregion
	}
}