using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Service;
using HectorSharp.Utils;
using Thrift;
using Apache.Cassandra;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Service
{
	/// <summary>
	/// Implementation of the client interface.
	/// </summary>
	public class CassandraClient : ICassandraClient
	{
		static Nullable<int> port = null;


		static ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.DCQUORUM;
		static FailoverStrategy DEFAULT_FAILOVER_STRATEGY = FailoverStrategy.ON_FAIL_TRY_ALL_AVAILABLE;

		static string PROP_CLUSTER_NAME = "cluster name";
		static string PROP_CONFIG_FILE = "config file";
		static string PROP_TOKEN_MAP = "token map";
		static string PROP_KEYSPACE = "keyspaces";
		static string PROP_VERSION = "version";

		//private static sealed Logger log = LoggerFactory.getLogger(CassandraClientImpl.class);

		// Serial number of the client used to track client creation for debug purposes
		static Counter serial = new Counter();
		long mySerial;

		Cassandra.Client cassandra; // The thrift object

		// List of known keyspaces 
		List<string> keyspaces;
		ConcurrentDictionary<string, Keyspace> keyspaceMap = new ConcurrentDictionary<string, Keyspace>();
		string clusterName;
		Dictionary<string, string> tokenMap;
		string configFile;
		string serverVersion;
		KeyspaceFactory keyspaceFactory;
		IKeyedObjectPool<Endpoint, ICassandraClient> pool;

		bool closed = false;
		bool hasErrors = false;

		public int Port { get { return port.HasValue ? port.Value : -1; } }
		
		#region ctor
		internal CassandraClient(Cassandra.Client thriftClient, KeyspaceFactory keyspaceFactory, Endpoint endpoint, IKeyedObjectPool<Endpoint, ICassandraClient> pool)
		{
			this.mySerial = serial.Increment();
			cassandra = thriftClient;
			this.keyspaceFactory = keyspaceFactory;

			if (endpoint == null)
				throw new ArgumentNullException("endpoint");

			if (!port.HasValue)
				port = endpoint.Port;

			if (port.Value != endpoint.Port)
			{
				if (this.pool != null)
					this.pool.Clear();
				port = endpoint.Port;
			}

			this.Endpoint = endpoint;
			this.pool = pool;
		} 
		#endregion

		public string ClusterName
		{
			get
			{
				if (clusterName == null)
					clusterName = GetStringProperty(PROP_CLUSTER_NAME);
				return clusterName;
			}
		}

		public string ConfigFile
		{
			get
			{
				if (configFile == null)
					configFile = GetStringProperty(PROP_CONFIG_FILE);
				return configFile;
			}
		}

		public IKeyspace GetKeyspace(string keySpaceName)
		{
			return GetKeyspace(keySpaceName, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_FAILOVER_STRATEGY);
		}

		public IKeyspace GetKeyspace(string keyspaceName, ConsistencyLevel consistencyLevel, FailoverPolicy failoverPolicy)
		{
			var keyspaceMapKey = BuildKeyspaceMapName(keyspaceName, consistencyLevel, failoverPolicy);
			var keyspace = keyspaceMap[keyspaceMapKey];
			if (keyspace == null)
			{
				if (Keyspaces.Contains(keyspaceName))
				{
					var keyspaceDesc = cassandra.describe_keyspace(keyspaceName);
					keyspace = (Keyspace)keyspaceFactory.Create(this, keyspaceName, keyspaceDesc,
						 consistencyLevel, failoverPolicy, pool);
					Keyspace tmp = null;
					if (!keyspaceMap.ContainsKey(keyspaceMapKey))
					{
						keyspaceMap.Add(keyspaceMapKey, keyspace);
						tmp = keyspaceMap[keyspaceMapKey];
					}
					if (tmp != null)
					{
						// There was another put that got here before we did.
						keyspace = tmp;
					}
				}
				else
				{
					throw new Exception("Requested key space not exist, keyspaceName=" + keyspaceName);
				}
			}
			return keyspace;
		}

		public IList<string> Keyspaces
		{
			get
			{
				if (keyspaces == null)
					keyspaces = cassandra.get_string_list_property(PROP_KEYSPACE);
				return keyspaces;
			}
		}

		public string GetStringProperty(string propertyName)
		{
			return cassandra.get_string_property(propertyName);
		}

		public Dictionary<string, string> GetTokenMap(bool fresh)
		{
			if (tokenMap == null || fresh)
			{
				tokenMap = new Dictionary<string, string>();
				string strTokens = GetStringProperty(PROP_TOKEN_MAP);
				// Parse the result of the form {"token1":"host1","token2":"host2"}
				strTokens = strTokens.Trim();
				string[] tokenPairs = strTokens.Split(',');
				foreach (string tokenPair in tokenPairs)
				{
					string[] keyValue = tokenPair.Split(':');
					string token = keyValue[0].Trim();
					string host = keyValue[1].Trim();
					tokenMap[token] = host;
				}

			}
			return tokenMap;
		}

		public string ServerVersion
		{
			get
			{
				if (serverVersion == null) serverVersion = GetStringProperty(PROP_VERSION);
				return serverVersion;
			}
		}
		/// <summary>
		/// Creates a unique map name for the keyspace and its consistency level
		/// </summary>
		/// <param name="keyspaceName"></param>
		/// <param name="consistencyLevel"></param>
		/// <param name="failoverPolicy"></param>
		/// <returns></returns>
		string BuildKeyspaceMapName(string keyspaceName, ConsistencyLevel consistencyLevel, FailoverPolicy failoverPolicy)
		{
			return string.Format("{0}[{1},{2}]", keyspaceName, consistencyLevel, failoverPolicy);
		}

		public Cassandra.Client Client { get { return cassandra; } }

		public Endpoint Endpoint { get; private set; }

		public void UpdateKnownEndpoints()
		{
			if (closed)
				return;

			// Iterate over all keyspaces and ask them to update known hosts
			foreach (var k in keyspaceMap)
				k.Value.UpdateKnownHosts();
		}

		public override string ToString()
		{
			return string.Format("CassandraClient<{0}:{1}-{2}>", Endpoint.Host, Endpoint.Port, mySerial);
		}

		public void MarkAsClosed()
		{
			closed = true;
		}

		public bool IsClosed { get { return closed; } }

		public IList<Endpoint> KnownEndpoints
		{
			get
			{
				var endpoints = new List<Endpoint>();
				return endpoints;
				/*
				if (closed) return endpoints;

				foreach (var keyspace in keyspaceMap.Values)
					endpoints.AddRange(keyspace.KnownEndpoints);

				return endpoints;*/
			}
		}

		public bool HasErrors { get { return hasErrors; } }

		public void MarkAsError()
		{
			hasErrors = true;
		}

		public void RemoveKeyspace(IKeyspace k)
		{
			string key = BuildKeyspaceMapName(k.Name, k.ConsistencyLevel, k.FailoverPolicy);
			keyspaceMap.Remove(key);
		}
	}
}