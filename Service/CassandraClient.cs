using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Service;
using HectorSharp.Utils;
using Thrift;
using Apache.Cassandra;

namespace HectorSharp.Service
{
	/**
	 * Implementation of the client interface.
	 *
	 * @author Matt Van Veenendaal (m@mattvv.com)
	 * @author Ran Tavory (rantav@gmail.com)
	 *
	 */
	/*package*/
	internal class CassandraClient : ICassandraClient
	{
		static ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.DCQUORUM;
		static FailoverPolicy DEFAULT_FAILOVER_POLICY = FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
		
		static String PROP_CLUSTER_NAME = "cluster name";
		static String PROP_CONFIG_FILE = "config file";
		static String PROP_TOKEN_MAP = "token map";
		static String PROP_KEYSPACE = "keyspaces";
		static String PROP_VERSION = "version";

		//@SuppressWarnings("unused")
		//private static sealed Logger log = LoggerFactory.getLogger(CassandraClientImpl.class);

		/** Serial number of the client used to track client creation for debug purposes */
		static Counter serial = new Counter();
		
		long mySerial;

		// The thrift object
		 Cassandra.Client cassandra;

		// List of known keyspaces 
		List<String> keyspaces;
		ConcurrentDictionary<String, KeyspaceImpl> keyspaceMap = new ConcurrentDictionary<String, KeyspaceImpl>();
		String clusterName;
		Dictionary<String, String> tokenMap;
		String configFile;
		String serverVersion;
		KeyspaceFactory keyspaceFactory;
		int port;
		String url;
		String ip;
		ICassandraClientPool clientPools;

		bool closed = false;
		bool hasErrors = false;

		public CassandraClient(Cassandra.Client thriftClient, KeyspaceFactory keyspaceFactory, String url, int port, ICassandraClientPool clientPools)
		{
			this.mySerial = serial.Increment();
			cassandra = thriftClient;
			this.keyspaceFactory = keyspaceFactory;
			this.port = port;
			this.url = url;
			ip = getIpString(url);
			this.clientPools = clientPools;
		}

		static String getIpString(String url)
		{
			return InetAddress.getByName(url).getHostAddress();
		}

		public String getClusterName()
		{
			if (clusterName == null)
				clusterName = getStringProperty(PROP_CLUSTER_NAME);
			return clusterName;
		}

		public String getConfigFile()
		{
			if (configFile == null)
				configFile = getStringProperty(PROP_CONFIG_FILE);
			return configFile;
		}

		public IKeyspace getKeyspace(String keySpaceName)
		{
			return getKeyspace(keySpaceName, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_FAILOVER_POLICY);
		}

		public IKeyspace getKeyspace(String keyspaceName, ConsistencyLevel consistencyLevel,
			 FailoverPolicy failoverPolicy)
		{
			String keyspaceMapKey = BuildKeyspaceMapName(keyspaceName, consistencyLevel, failoverPolicy);
			KeyspaceImpl keyspace = keyspaceMap[keyspaceMapKey];
			if (keyspace == null)
			{
				if (getKeyspaces().Contains(keyspaceName))
				{
					var keyspaceDesc = cassandra.describe_keyspace(keyspaceName);
					keyspace = (KeyspaceImpl)keyspaceFactory.create(this, keyspaceName, keyspaceDesc,
						 consistencyLevel, failoverPolicy, clientPools);
					KeyspaceImpl tmp = null;
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
					throw new Exception(
						 "Requested key space not exist, keyspaceName=" + keyspaceName);
				}
			}
			return keyspace;
		}

		public List<String> getKeyspaces()
		{
			if (keyspaces == null)
				keyspaces = cassandra.get_string_list_property(PROP_KEYSPACE);
			return keyspaces;
		}

		public String getStringProperty(String propertyName)
		{
			return cassandra.get_string_property(propertyName);
		}

		public Dictionary<String, String> getTokenMap(boolean fresh)
		{
			if (tokenMap == null || fresh)
			{
				tokenMap = new Dictionary<String, String>();
				String strTokens = getStringProperty(PROP_TOKEN_MAP);
				// Parse the result of the form {"token1":"host1","token2":"host2"}
				strTokens = trimBothSides(strTokens);
				String[] tokenPairs = strTokens.Split(',');
				foreach (string tokenPair in tokenPairs)
				{
					String[] keyValue = tokenPair.Split(':');
					String token = trimBothSides(keyValue[0]);
					String host = trimBothSides(keyValue[1]);
					tokenMap[token] = host;
				}

			}
			return tokenMap;
		}

		public String getServerVersion()
		{
			if (serverVersion == null)
				serverVersion = getStringProperty(PROP_VERSION);
			return serverVersion;
		}

		/**
		 * Creates a unique map name for the keyspace and its consistency level
		 * @param keyspaceName
		 * @param consistencyLevel
		 * @return
		 */
		private String BuildKeyspaceMapName(String keyspaceName, ConsistencyLevel consistencyLevel, FailoverPolicy failoverPolicy)
		{
			return String.Format("{0}[{1},{2}]", keyspaceName, consistencyLevel, failoverPolicy);
		}

		public Cassandra.Client Client { get { return cassandra; } }

		/**
		 * Trims the string, one char from each side.
		 * For example, this: asdf becomes this: sd
		 * Useful in those cases:  "asdf" => asdf
		 * @param str
		 * @return
		 */
		private String trimBothSides(String str)
		{
			return str.TrimStart().TrimEnd();
		}

		public int Port { get { return port; } }
		public String Url { get { return url; } }

		public void updateKnownHosts()
		{
			if (closed)
				return;
		
			// Iterate over all keyspaces and ask them to update known hosts
			foreach (var k in keyspaceMap)
				k.Value.updateKnownHosts();
		}

		public override string ToString()
		{
			return string.Format("CassandraClient<{0}:{1}-{2}>", Url, Port, mySerial);
		}

		public void markAsClosed()
		{
			closed = true;
		}

		public bool IsClosed { get { return closed; } }

		public List<String> getKnownHosts()
		{
			var hosts = new List<String>();
			if (closed)
				return hosts;
			
			// Iterate over all keyspaces and ask them to update known hosts
			foreach (var k in keyspaceMap.Values)
				hosts.AddRange(k.getKnownHosts());
			
			return hosts;
		}

		public String IP { get { return ip; } }

		public bool HasErrors { get { return hasErrors; } }

		public void markAsError()
		{
			hasErrors = true;
		}

		public void removeKeyspace(IKeyspace k)
		{
			String key = BuildKeyspaceMapName(k.Name, k.ConsistencyLevel, k.FailoverPolicy);
			keyspaceMap.Remove(key);
		}
	}
}