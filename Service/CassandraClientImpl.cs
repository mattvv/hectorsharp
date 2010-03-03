using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Service;
using HectorSharp.Utils;
using Thrift;

namespace HectorSharp.Service
{
/**
 * Implementation of the client interface.
 *
 * @author Matt Van Veenendaal (m@mattvv.com)
 * @author Ran Tavory (rantav@gmail.com)
 *
 */
/*package*/ class CassandraClientImpl : CassandraClient {
 
  sealed static String PROP_CLUSTER_NAME = "cluster name";
  sealed static String PROP_CONFIG_FILE = "config file";
  sealed static String PROP_TOKEN_MAP = "token map";
  sealed static String PROP_KEYSPACE = "keyspaces";
  sealed static String PROP_VERSION = "version";
 
  //@SuppressWarnings("unused")
  //private static sealed Logger log = LoggerFactory.getLogger(CassandraClientImpl.class);
 
  /** Serial number of the client used to track client creation for debug purposes */
  static sealed AtomicLong serial = new AtomicLong(0); //wtf is this AtomicLong?!
 
  sealed long mySerial;
 
  /** The thrift object */
  sealed Cassandra.Client cassandra;
 
  /** List of known keyspaces */
  List<String> keyspaces;
 
  sealed ConcurrentDictionary<String, KeyspaceImpl> keyspaceMap =
      new ConcurrentDictionary<String, KeyspaceImpl>();
 
  String clusterName;
 
  Map<String, String> tokenMap;
 
  String configFile;
 
  String serverVersion;
 
  sealed KeyspaceFactory keyspaceFactory;
 
  sealed int port;
 
  sealed String url;
  sealed String ip;
 
  sealed CassandraClientPool clientPools;
 
  boolean closed = false;
  boolean hasErrors = false;
 
  public CassandraClientImpl(Cassandra.Client cassandraThriftClient,
      KeyspaceFactory keyspaceFactory, String url, int port, CassandraClientPool clientPools)
      {
    this.mySerial = serial.incrementAndGet();
    cassandra = cassandraThriftClient;
    this.keyspaceFactory = keyspaceFactory;
    this.port = port;
    this.url = url;
    ip = getIpString(url);
    this.clientPools = clientPools;
  }
 
  static String getIpString(String url) {
    return InetAddress.getByName(url).getHostAddress();
  }
 
  //@Override
  public String getClusterName() {
    if (clusterName == null) {
      clusterName = getStringProperty(PROP_CLUSTER_NAME);
    }
    return clusterName;
  }
 
  //@Override
  public String getConfigFile() {
    if (configFile == null) {
      configFile = getStringProperty(PROP_CONFIG_FILE);
    }
    return configFile;
  }
 
  //@Override
  public Keyspace getKeyspace(String keySpaceName)  {
    return getKeyspace(keySpaceName, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_FAILOVER_POLICY);
  }
 
  //@Override
  public Keyspace getKeyspace(String keyspaceName, int consistencyLevel,
      FailoverPolicy failoverPolicy) {
    String keyspaceMapKey = buildKeyspaceMapName(keyspaceName, consistencyLevel, failoverPolicy);
    KeyspaceImpl keyspace = keyspaceMap.get(keyspaceMapKey);
    if (keyspace == null) {
      if (getKeyspaces().contains(keyspaceName)) {
        Map<String, Map<String, String>> keyspaceDesc = cassandra.describe_keyspace(keyspaceName);
        keyspace = (KeyspaceImpl) keyspaceFactory.create(this, keyspaceName, keyspaceDesc,
            consistencyLevel, failoverPolicy, clientPools);
        KeyspaceImpl tmp = keyspaceMap.putIfAbsent(keyspaceMapKey , keyspace);
        if (tmp != null) {
          // There was another put that got here before we did.
          keyspace = tmp;
        }
      }else{
        throw new Exception(
            "Requested key space not exist, keyspaceName=" + keyspaceName);
      }
    }
    return keyspace;
  }
 
  //@Override
  public List<String> getKeyspaces() {
    if (keyspaces == null) {
      keyspaces = cassandra.get_string_list_property(PROP_KEYSPACE);
    }
    return keyspaces;
  }
 
  //@Override
  public String getStringProperty(String propertyName) {
    return cassandra.get_string_property(propertyName);
  }
 
  //@Override
  public Map<String, String> getTokenMap(boolean fresh)  {
    if (tokenMap == null || fresh) {
      tokenMap = new Dictionary<String, String>();
      String strTokens = getStringProperty(PROP_TOKEN_MAP);
      // Parse the result of the form {"token1":"host1","token2":"host2"}
      strTokens = trimBothSides(strTokens);
      String[] tokenPairs = strTokens.Split(',');
      foreach (string tokenPair in tokenPairs) {
        String[] keyValue = tokenPair.Split(':');
        String token = trimBothSides(keyValue[0]);
        String host = trimBothSides(keyValue[1]);
        tokenMap.put(token, host);
      }
    }
    return tokenMap;
  }
 
  //@Override
  public String getServerVersion() {
    if (serverVersion == null) {
      serverVersion = getStringProperty(PROP_VERSION);
    }
    return serverVersion;
   }
 
  /**
   * Creates a unique map name for the keyspace and its consistency level
   * @param keyspaceName
   * @param consistencyLevel
   * @return
   */
  private String buildKeyspaceMapName(String keyspaceName, int consistencyLevel,
      FailoverPolicy failoverPolicy) {
    StringBuilder b = new StringBuilder(keyspaceName);
    b.Append('[');
    b.Append(consistencyLevel);
    b.Append(',');
    b.Append(failoverPolicy);
    b.Append(']');
    return b.ToString();
  }
 
  //@Override
  public Client getCassandra() {
    return cassandra;
  }
 
  /**
   * Trims the string, one char from each side.
   * For example, this: asdf becomes this: sd
   * Useful in those cases:  "asdf" => asdf
   * @param str
   * @return
   */
  private String trimBothSides(String str) {
    str = str.Substring(1);
    str = str.Substring(0, str.Length - 1);
    return str;
  }
 
  //@Override
  public int getPort() {
    return port;
  }
 
  //@Override
  public String getUrl() {
    return url;
  }
 
  //@Override
  public void updateKnownHosts() {
    if (closed) {
      return;
    }
    // Iterate over all keyspaces and ask them to update known hosts
    foreach (KeyspaceImpl k in keyspaceMap.All) {
      k.updateKnownHosts();
    }
  }
 
  //@Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.Append("CassandraClient<");
    b.Append(getUrl());
    b.Append(":");
    b.Append(getPort());
    b.Append("-");
    b.Append(mySerial);
    b.Append(">");
    return b.ToString();
  }
 
  //@Override
  public void markAsClosed() {
    closed = true;
  }
 
  //@Override
  public boolean isClosed() {
    return closed;
  }
 
  //@Override
  public Set<String> getKnownHosts() {
    Set<String> hosts = new HashSet<String>();
    if (closed) {
      return hosts;
    }
    // Iterate over all keyspaces and ask them to update known hosts
    for (KeyspaceImpl k: keyspaceMap.values()) {
      hosts.addAll(k.getKnownHosts());
    }
    return hosts;
  }
 
  //@Override
  public String getIp() {
    return ip;
  }
 
  //@Override
  public boolean hasErrors() {
    return hasErrors;
  }
 
  //@Override
  public void markAsError() {
    hasErrors = true;
  }
 
  //@Override
  public void removeKeyspace(Keyspace k) {
    String key = buildKeyspaceMapName(k.getName(), k.getConsistencyLevel(), k.getFailoverPolicy());
    keyspaceMap.Remove(key);
  }
}
