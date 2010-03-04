using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using Thrift;
using Thrift.Transport;

namespace HectorSharp.Service
{
	/**
	 * Implamentation of a keyspace
	 *
	 * @author Ran Tavory (rantav@gmail.com)
	 *
	 */

	class KeyspaceImpl : IKeyspace
	{
		// constants
		public static String CF_TYPE = "Type";
		public static String CF_TYPE_STANDARD = "Standard";
		public static String CF_TYPE_SUPER = "Super";

		//private static sealed Logger log = LoggerFactory.getLogger(KeyspaceImpl.class);

		ICassandraClient client;
		Cassandra.Client cassandra; // The cassandra thrift proxy
		sealed Dictionary<String, Dictionary<String, String>> keyspaceDesc;
		sealed ConsistencyLevel consistency;
		sealed FailoverPolicy failoverPolicy;
		/** List of all known remote cassandra nodes */
		List<String> knownHosts = new List<String>();
		sealed ICassandraClientPool clientPools;
		sealed CassandraClientMonitor monitor;

		public KeyspaceImpl(ICassandraClient client, String keyspaceName,
	 Dictionary<String, Dictionary<String, String>> keyspaceDesc, ConsistencyLevel consistencyLevel,
	 FailoverPolicy failoverPolicy, ICassandraClientPool clientPools, CassandraClientMonitor monitor)
		{
			this.client = client;
			this.consistency = consistencyLevel;
			this.keyspaceDesc = keyspaceDesc;
			this.Name = keyspaceName;
			this.cassandra = client.getCassandra();
			this.failoverPolicy = failoverPolicy;
			this.clientPools = clientPools;
			this.monitor = monitor;
			initFailover();
		}

		//Override
		public void batchInsert(String key, Dictionary<String, List<Column>> columnMap, Dictionary<String, List<SuperColumn>> superColumnMap)
		{
			if (columnMap == null && superColumnMap == null)
				throw new Exception("columnMap and SuperColumnMap can not be null at same time");

			int size = (columnMap == null ? 0 : columnMap.Count) + (columnMap == null ? 0 : columnMap.Count());
			Dictionary<String, List<ColumnOrSuperColumn>> cfmap = new Dictionary<String, List<ColumnOrSuperColumn>>(size * 2);

			if (columnMap != null)
				foreach (var map in columnMap)
					cfmap.Add(map.Key, getSoscList(map.Value));

			if (superColumnMap != null)
				foreach (var map in superColumnMap)
					cfmap.Add(map.Key, getSoscSuperList(map.Value));

			//todo: fix Operation execute event
			/*
		 Operation<Void> op = new Operation<Void>(Counter.WRITE_FAIL)
		 {
			//Override
			public void execute(Client cassandra)  {
			  cassandra.batch_insert(keyspaceName, key, cfmap, consistency);
			  return null;
			}
		 };
		 operateWithFailover(op);*/
		}

		//Override
		public int getCount(String key, ColumnParent columnParent)
		{
			//todo: fix Operation execute event
			/*
		 Operation<Integer> op = new Operation<Integer>(Counter.READ_FAIL) {
			//Override
			public int execute(Client cassandra) {
			  return cassandra.get_count(keyspaceName, key, columnParent, consistency);
			}
		 };
		 operateWithFailover(op);
		 return op.getResult();*/
			return 0;
		}

		//Override
		public Dictionary<String, List<Column>> getRangeSlice(ColumnParent columnParent,
			 SlicePredicate predicate, String start, String finish, int count)
		{
			//Operation<Map<String, List<Column>>> op = new Operation<Map<String, List<Column>>>(
			//    Counter.READ_FAIL) {
			//  @Override
			//  public Map<String, List<Column>> execute(Client cassandra) throws InvalidRequestException,
			//      UnavailableException, TException, TimedOutException {
			//    List<KeySlice> keySlices = cassandra.get_range_slice(keyspaceName, columnParent, predicate,
			//        start, finish, count, consistency);
			//    if (keySlices == null || keySlices.isEmpty()) {
			//      return Collections.emptyMap();
			//    }
			//    Map<String, List<Column>> ret = new HashMap<String, List<Column>>(keySlices.size());
			//    for (KeySlice keySlice : keySlices) {
			//      ret.put(keySlice.getKey(), getColumnList(keySlice.getColumns()));
			//    }
			//    return ret;
			//  }
			//};
			//operateWithFailover(op);
			//return op.getResult();
			return new Dictionary<string, List<Column>>();
		}

		//Override
		public Dictionary<String, List<SuperColumn>> getSuperRangeSlice(ColumnParent columnParent,
			 SlicePredicate predicate, String start, String finish, int count)
		// throws InvalidRequestException, UnavailableException, TException, TimedOutException 
		{
			//Operation<Map<String, List<SuperColumn>>> op = new Operation<Map<String, List<SuperColumn>>>(
			//    Counter.READ_FAIL) {
			//  @Override
			//  public Map<String, List<SuperColumn>> execute(Client cassandra)
			//      throws InvalidRequestException, UnavailableException, TException, TimedOutException {
			//    List<KeySlice> keySlices = cassandra.get_range_slice(keyspaceName, columnParent, predicate,
			//        start, finish, count, consistency);
			//    if (keySlices == null || keySlices.isEmpty()) {
			//      return Collections.emptyMap();
			//    }
			//    Map<String, List<SuperColumn>> ret = new HashMap<String, List<SuperColumn>>(
			//        keySlices.size());
			//    for (KeySlice keySlice : keySlices) {
			//      ret.put(keySlice.getKey(), getSuperColumnList(keySlice.getColumns()));
			//    }
			//    return ret;
			//  }
			//};
			//operateWithFailover(op);
			//return op.getResult();
			return new Dictionary<string, List<SuperColumn>>();
		}

		//Override
		public List<Column> getSlice(String key, ColumnParent columnParent,
			 SlicePredicate predicate)
		//throws InvalidRequestException, NotFoundException,
		//UnavailableException, TException, TimedOutException
		{
			//Operation<List<Column>> op = new Operation<List<Column>>(Counter.READ_FAIL) {
			//  @Override
			//  public List<Column> execute(Client cassandra) throws InvalidRequestException,
			//      UnavailableException, TException, TimedOutException {
			//    List<ColumnOrSuperColumn> cosclist = cassandra.get_slice(keyspaceName, key, columnParent,
			//        predicate, consistency);

			//    if (cosclist == null) {
			//      return null;
			//    }
			//    ArrayList<Column> result = new ArrayList<Column>(cosclist.size());
			//    for (ColumnOrSuperColumn cosc : cosclist) {
			//      result.add(cosc.getColumn());
			//    }
			//    return result;
			//  }
			//};
			//operateWithFailover(op);
			//return op.getResult();
			return new List<Column>();
		}

		//Override
		public SuperColumn getSuperColumn(String key, ColumnPath columnPath)
		//throws InvalidRequestException, NotFoundException, UnavailableException, TException,
		//TimedOutException 
		{
			return getSuperColumn(key, columnPath, false, Int32.MaxValue);
		}

		//Override
		public SuperColumn getSuperColumn(String key, ColumnPath columnPath,
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
		public List<SuperColumn> getSuperSlice(String key, ColumnParent columnParent,
			 SlicePredicate predicate)
		//throws InvalidRequestException, NotFoundException,
		//UnavailableException, TException, TimedOutException 
		{
			//Operation<List<SuperColumn>> op = new Operation<List<SuperColumn>>(Counter.READ_FAIL) {
			//  @Override
			//  public List<SuperColumn> execute(Client cassandra) throws InvalidRequestException,
			//      UnavailableException, TException, TimedOutException {
			//    List<ColumnOrSuperColumn> cosclist = cassandra.get_slice(keyspaceName, key, columnParent,
			//        predicate, consistency);
			//    if (cosclist == null) {
			//      return null;
			//    }
			//    ArrayList<SuperColumn> result = new ArrayList<SuperColumn>(cosclist.size());
			//    for (ColumnOrSuperColumn cosc : cosclist) {
			//      result.add(cosc.getSuper_column());
			//    }
			//    return result;
			//  }
			//};
			//operateWithFailover(op);
			//return op.getResult();
			return new List<SuperColumn>();
		}

		//Override
		public void insert(String key, ColumnPath columnPath, byte[] value)
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
		public Dictionary<String, Column> multigetColumn(List<String> keys, ColumnPath columnPath)
		// throws InvalidRequestException, UnavailableException, TException, TimedOutException 
		{
			//valideColumnPath(columnPath);

			//Operation<Map<String, Column>> op = new Operation<Map<String, Column>>(Counter.READ_FAIL) {
			//  @Override
			//  public Map<String, Column> execute(Client cassandra) throws InvalidRequestException,
			//      UnavailableException, TException, TimedOutException {
			//    Map<String, ColumnOrSuperColumn> cfmap = cassandra.multiget(keyspaceName, keys, columnPath,
			//        consistency);
			//    if (cfmap == null || cfmap.isEmpty()) {
			//      return Collections.emptyMap();
			//    }
			//    Map<String, Column> result = new HashMap<String, Column>();
			//    for (Map.Entry<String, ColumnOrSuperColumn> entry : cfmap.entrySet()) {
			//      result.put(entry.getKey(), entry.getValue().getColumn());
			//    }
			//    return result;
			//  }
			//};
			//operateWithFailover(op);
			//return op.getResult();
			return new Dictionary<string, Column>();
		}

		//Override
		public Dictionary<String, List<Column>> multigetSlice(List<String> keys,
			  ColumnParent columnParent, SlicePredicate predicate)
		// throws InvalidRequestException, UnavailableException, TException, TimedOutException 
		{
			//Operation<Map<String, List<Column>>> getCount = new Operation<Map<String, List<Column>>>(
			//    Counter.READ_FAIL) {
			//  @Override
			//  public Map<String, List<Column>> execute(Client cassandra) throws InvalidRequestException,
			//      UnavailableException, TException, TimedOutException {
			//    Map<String, List<ColumnOrSuperColumn>> cfmap = cassandra.multiget_slice(keyspaceName, keys,
			//        columnParent, predicate, consistency);

			//    Map<String, List<Column>> result = new HashMap<String, List<Column>>();
			//    for (Map.Entry<String, List<ColumnOrSuperColumn>> entry : cfmap.entrySet()) {
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
		public Dictionary<String, SuperColumn> multigetSuperColumn(List<String> keys, ColumnPath columnPath)
		//throws InvalidRequestException, UnavailableException, TException, TimedOutException 
		{
			return multigetSuperColumn(keys, columnPath, false, Int32.MaxValue);
		}

		//Override
		public Dictionary<String, SuperColumn> multigetSuperColumn(List<String> keys, ColumnPath columnPath,
			 bool reversed, int size)
		///throws InvalidRequestException, UnavailableException, TException, TimedOutException 
		{
			//valideSuperColumnPath(columnPath);

			// only can get supercolumn by multigetSuperSlice
			ColumnParent clp = new ColumnParent(columnPath.Column_family, columnPath.Super_column);
			SliceRange sr = new SliceRange(new byte[0], new byte[0], reversed, size);
			SlicePredicate sp = new SlicePredicate(null, sr);
			var sclist = multigetSuperSlice(keys, clp, sp);

			if (sclist == null || sclist.Count == 0)
			{
				return new Dictionary<string, SuperColumn>();
			}

			var result = new Dictionary<String, SuperColumn>();
			foreach (var entry in sclist)
			{
				var sclistByKey = entry.Value;
				if (sclistByKey.Count > 0)
					result.Add(entry.Key, sclistByKey[0]);
			}
			return result;
		}

		//Override
		public Dictionary<String, List<SuperColumn>> multigetSuperSlice(List<String> keys,
			 ColumnParent columnParent, SlicePredicate predicate)
		//throws InvalidRequestException, UnavailableException, TException, TimedOutException 
		{
			//Operation<Map<String, List<SuperColumn>>> getCount = new Operation<Map<String, List<SuperColumn>>>(
			//    Counter.READ_FAIL) {
			//  @Override
			//  public Map<String, List<SuperColumn>> execute(Client cassandra)
			//      throws InvalidRequestException, UnavailableException, TException, TimedOutException {
			//    Map<String, List<ColumnOrSuperColumn>> cfmap = cassandra.multiget_slice(keyspaceName, keys,
			//        columnParent, predicate, consistency);

			//    // if user not given super column name, the multiget_slice will return
			//    // List
			//    // filled with
			//    // super column, if user given a column name, the return List will
			//    // filled
			//    // with column,
			//    // this is a bad interface design.
			//    if (columnParent.getSuper_column() == null) {
			//      Map<String, List<SuperColumn>> result = new HashMap<String, List<SuperColumn>>();
			//      for (Map.Entry<String, List<ColumnOrSuperColumn>> entry : cfmap.entrySet()) {
			//        result.put(entry.getKey(), getSuperColumnList(entry.getValue()));
			//      }
			//      return result;
			//    } else {
			//      Map<String, List<SuperColumn>> result = new HashMap<String, List<SuperColumn>>();
			//      for (Map.Entry<String, List<ColumnOrSuperColumn>> entry : cfmap.entrySet()) {
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

		//Override
		public void remove(String key, ColumnPath columnPath)
		//throws InvalidRequestException,
		//UnavailableException, TException, TimedOutException 
		{
			//Operation<Void> op = new Operation<Void>(Counter.WRITE_FAIL) {
			//  @Override
			//  public Void execute(Client cassandra) throws InvalidRequestException, UnavailableException,
			//      TException, TimedOutException {
			//    cassandra.remove(keyspaceName, key, columnPath, createTimeStamp(), consistency);
			//    return null;
			//  }
			//};
			//operateWithFailover(op);

		}

		//Override
		public String getName()
		{
			return Name;
		}

		public string Name { get; private set; }


		//Override
		public Dictionary<String, Dictionary<String, String>> describeKeyspace()
		//throws NotFoundException, TException 
		{
			return keyspaceDesc;
		}

		//Override
		public ICassandraClient getClient()
		{
			return client;
		}

		//Override
		public Column getColumn(String key, ColumnPath columnPath)
		//      throws InvalidRequestException, NotFoundException, UnavailableException, TException,
		//      TimedOutException 
		{
			//valideColumnPath(columnPath);
			/*
				Operation<Column> op = new Operation<Column>(Counter.READ_FAIL) {
				  @Override
				  public Column execute(Client cassandra) throws InvalidRequestException, UnavailableException,
						TException, TimedOutException {
					 ColumnOrSuperColumn cosc;
					 try {
						cosc = cassandra.get(keyspaceName, key, columnPath, consistency);
					 } catch (NotFoundException e) {
						setException(e);
						return null;
					 }
					 return cosc == null ? null : cosc.getColumn();
				  }
 
				};
				operateWithFailover(op);
				if (op.hasException()) {
				  throw op.getException();
				}
				return op.getResult();
			*/
			return new Column();
		}

		//Override
		public int getConsistencyLevel()
		{
			return consistency;
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
			String cf = columnPath.Column_family;
			Dictionary<String, String> cfdefine;
			if ((cfdefine = keyspaceDesc[cf]) != null)
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

		/**
		 * Make sure that the given column path is a SuperColumn in the DB, Throws an
		 * exception if it's not.
		 *
		 * @throws InvalidRequestException
		 */
		private void valideSuperColumnPath(ColumnPath columnPath)
		//throws InvalidRequestException 
		{
			String cf = columnPath.Column_family;
			Dictionary<String, String> cfdefine;
			if ((cfdefine = keyspaceDesc[cf]) != null && cfdefine[CF_TYPE].Equals(CF_TYPE_SUPER)
				 && columnPath.Super_column != null)
			{
				return;
			}
			throw new InvalidRequestException(
				 "Invalid super column or super column family does not exist: " + cf);
		}

		static List<ColumnOrSuperColumn> getSoscList(List<Column> columns)
		{
			List<ColumnOrSuperColumn> list = new List<ColumnOrSuperColumn>(columns.Count);
			foreach (var col in columns)
			{
				list.Add(new ColumnOrSuperColumn(col, null));
			}
			return list;
		}

		static List<ColumnOrSuperColumn> getSoscSuperList(List<SuperColumn> columns)
		{
			var list = new List<ColumnOrSuperColumn>();
			foreach (var col in columns)
			{
				list.Add(new ColumnOrSuperColumn(null, col));
			}
			return list;
		}

		static List<Column> getColumnList(List<ColumnOrSuperColumn> columns)
		{
			var list = new List<Column>();
			foreach (var col in columns)
			{
				list.Add(col.Column);
			}
			return list;
		}

		private static List<SuperColumn> getSuperColumnList(List<ColumnOrSuperColumn> columns)
		{
			var list = new List<SuperColumn>();
			foreach (var col in columns)
			{
				list.Add(col.Super_column);
			}
			return list;
		}

		//Override
		public FailoverPolicy FailoverPolicy
		{
			get { return failoverPolicy; }
		}

		/**
		 * Initializes the ring info so we can handle failover if this happens later.
		 *
		 * @throws TException
		 */
		private void initFailover()
		{
			if (failoverPolicy == FailoverPolicy.FAIL_FAST)
			{
				knownHosts.Clear();
				knownHosts.Add(client.getUrl());
				return;
			}
			// learn about other cassandra hosts in the ring
			updateKnownHosts();
		}

		/**
		 * Uses the current known host to query about all other hosts in the ring.
		 *
		 * @throws TException
		 */
		public void updateKnownHosts()
		{
			// When update starts we only know of this client, nothing else
			knownHosts.Clear();
			knownHosts.Add(getClient().getUrl());

			// Now query for more hosts. If the query fails, then even this client is
			// now "known"
			try
			{
				var map = getClient().getTokenMap(true);
				knownHosts.Clear();
				foreach (var entry in map)
				{
					knownHosts.Add(entry.Value);
				}
			}
			catch// (TException e) 
			{
				knownHosts.Clear();
				//log.error("Cannot query tokenMap; Keyspace {} is now disconnected", toString());
			}
		}

		/**
		 * Updates the client member and cassandra member to the next host in the
		 * ring.
		 *
		 * Returns the current client to the pool and retreives a new client from the
		 * next pool.
		 */
		private void skipToNextHost()
		{
			//log.info("Skipping to next host. Current host is: {}", client.getUrl());
			try
			{
				clientPools.invalidateClient(client);
				client.removeKeyspace(this);
			}
			catch// (Exception e)
			{
				//log.error("Unable to invalidate client {}. Will continue anyhow.", client);
			}

			String nextHost = getNextHost(client.getUrl(), client.getIp());
			if (nextHost == null)
			{
				//log.error("Unable to find next host to skip to at {}", toString());
				throw new Exception("Unable to failover to next host");
			}
			// assume they use the same port
			client = clientPools.borrowClient(nextHost, client.getPort());
			cassandra = client.getCassandra();
			monitor.incCounter(CassandraClientMonitor.ClientCounter.SKIP_HOST_SUCCESS);
			//log.info("Skipped host. New host is: {}", client.getUrl());
		}

		/**
		 * Finds the next host in the knownHosts. Next is the one after the given url
		 * (modulo the number of elemens in the list)
		 *
		 * @return URL of the next presumably available host. null if none can be
		 *         found.
		 */
		String getNextHost(String url, String ip)
		{
			int size = knownHosts.Count;
			if (size < 1)
			{
				return null;
			}
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

		/**
		 * Performs the operation and retries in in case the class is configured for
		 * retries, and there are enough hosts to try and the error was
		 * {@link TimedOutException}.
		 */
		//@SuppressWarnings("unchecked")
		void operateWithFailover(Operation op)
		{
			int retries = Math.Min(failoverPolicy.getNumRetries() + 1, knownHosts.Count);
			try
			{
				while (retries > 0)
				{
					--retries;
					// log.debug("Performing operation on {}; retries: {}", client.getUrl(), retries);
					try
					{
						// Perform operation and save its result value
						op.executeAndSetResult(cassandra);
						// hmmm don't count success, there are too many...
						// monitor.incCounter(op.successCounter);
						//   log.debug("Operation succeeded on {}", client.getUrl());
						return;
					}
					catch (TimedOutException e)
					{
						//   log.warn("Got a TimedOutException from {}. Num of retries: {}", client.getUrl(), retries);
						if (retries == 0)
						{
							throw e;
						}
						else
						{
							skipToNextHost();
							monitor.incCounter(CassandraClientMonitor.ClientCounter.RECOVERABLE_TIMED_OUT_EXCEPTIONS);
						}
					}
					catch (UnavailableException e)
					{
						//  log.warn("Got a UnavailableException from {}. Num of retries: {}", client.getUrl(),
						//      retries);
						if (retries == 0)
						{
							throw e;
						}
						else
						{
							skipToNextHost();
							monitor.incCounter(CassandraClientMonitor.ClientCounter.RECOVERABLE_UNAVAILABLE_EXCEPTIONS);
						}
					}
					catch (TTransportException e)
					{
						//   log.warn("Got a TTransportException from {}. Num of retries: {}", client.getUrl(),
						//       retries);
						if (retries == 0)
						{
							throw e;
						}
						else
						{
							skipToNextHost();
							monitor.incCounter(CassandraClientMonitor.ClientCounter.RECOVERABLE_TRANSPORT_EXCEPTIONS);
						}
					}
				}
			}
			catch (InvalidRequestException e)
			{
				monitor.incCounter(op.failCounter);
				throw e;
			}
			catch (UnavailableException e)
			{
				monitor.incCounter(op.failCounter);
				throw e;
			} /*catch (TException e) {
      monitor.incCounter(op.failCounter);
      throw e;
    } */
			catch (TimedOutException e)
			{
				monitor.incCounter(op.failCounter);
				throw e;
			}
			catch (PoolExhaustedException e)
			{
				//log.warn("Pool is exhausted", e);
				monitor.incCounter(op.failCounter);
				monitor.incCounter(CassandraClientMonitor.ClientCounter.POOL_EXHAUSTED);
				throw new UnavailableException();
			} /*catch (IllegalStateException e) {
      //log.error("Client Pool is already closed, cannot obtain new clients.", e);
      monitor.incCounter(op.failCounter);
      throw new UnavailableException();
    } */
			catch (Exception)
			{
				//log.error("Cannot retry failover, got an Exception", e);
				monitor.incCounter(op.failCounter);
				throw new UnavailableException();
			}
		}

		public Set<String> getKnownHosts()
		{
			List<String> hosts = new List<String>();
			hosts.AddRange(knownHosts);
			return hosts;
		}

		//@Override
		public String toString()
		{
			StringBuilder b = new StringBuilder();
			b.Append("KeyspaceImpl<");
			b.Append(getClient());
			b.Append(">");
			return b.ToString();
		}

		delegate T OperationExecuteHandler<T>(Cassandra.Client cassandra);

		/// <summary>
		/// Defines the interface of an operation performed on cassandra
		/// </summary>
		/// <typeparam name="T">
		/// The result type of the operation (if it has a result), such as the
		/// result of get_count or get_column
		/// </typeparam>
		class Operation<T> where T : class
		{
			/** Counts failed attempts */
			static Counter failCounter;

			OperationExecuteHandler<T> executeHandler;
			NotFoundException exception;

			public Operation(CassandraClientMonitor.ClientCounter failedCounter, OperationExecuteHandler<T> executeHandler)
			{
				Operation<T>.failCounter = failedCounter;
				this.executeHandler = executeHandler;
			}

			public T Result { get; set; }

			/// <summary>
			/// Performs the operation on the given cassandra instance.
			/// </summary>
			/// <param name="cassandra">client</param>
			/// <returns>null if no execute handler</returns>
			public T Execute(Cassandra.Client cassandra)
			{
				if (executeHandler != null)
					throw new ApplicationException("Execution Handler was null");

				return executeHandler(cassandra);
			}

			public void ExecuteAndSetResult(Cassandra.Client cassandra)
			{
				Result = Execute(cassandra);
			}

			public bool HasError { get { return exception != null; } }
			public NotFoundException Error 
			{ 
				get { return exception; }
				set { exception = value; }
			}
		}


	}
}