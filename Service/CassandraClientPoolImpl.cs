using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using Thrift;

namespace HectorSharp.Service
{
/**
 * We declare this pool as enum to make sure it stays a singlton in the system so clients may
 * efficiently be reused.
 *
 * @author Matt Van Veenendaal (m@mattvv.com)
 * @author Ran Tavory (ran@outbain.com) [ Original Java version ]
 *
 */
 class CassandraClientPoolImpl : ICassandraClientPool {
 
  //private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolImpl.class);
  /**
   * Mapping b/w the host identifier (url:port) and the pool used to store connections to it.
   */
  private sealed Map<PoolKey, CassandraClientPoolByHost> pools;
 
  private sealed CassandraClientMonitor clientMonitor;
 
  public CassandraClientPoolImpl(CassandraClientMonitor clientMonitor) {
    pools = new Dictionary<PoolKey, CassandraClientPoolByHost>();
    this.clientMonitor = clientMonitor;
  }
 
  //Override
  public ICassandraClient borrowClient(String url, int port)
  {
    return getPool(url, port).borrowClient();
  }
 
  //Override
  public Set<String> getExhaustedPoolNames() {
    Set<String> hosts = new HashSet<String>();
    for (CassandraClientPoolByHost pool: pools.values()) {
      if (pool.isExhausted()) {
        hosts.add(pool.getName());
      }
    }
    return hosts;
  }
 
  //Override
  public int getNumActive() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      count += pool.getNumActive();
    }
    return count;
  }
 
  //Override
  public int getNumBlockedThreads() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      count += pool.getNumBlockedThreads();
    }
    return count;
  }
 
  //Override
  public int getNumExhaustedPools() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      if (pool.isExhausted()) {
        ++count;
      }
    }
    return count;
  }
 
  //Override
  public int getNumIdle() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      count += pool.getNumIdle();
    }
    return count;
  }
 
  //Override
  public int getNumPools() {
    return pools.size();
  }
 
  public CassandraClientPoolByHost getPool(String url, int port) {
    PoolKey key = new PoolKey(url, port);
    CassandraClientPoolByHost pool = pools.get(key);
    if (pool == null) {
      synchronized (pools) {
        pool = pools.get(key);
        if (pool == null) {
          pool = new CassandraClientPoolByHostImpl(url, port, key.name, this, clientMonitor);
          pools.put(key, pool);
        }
      }
    }
    return pool;
  }
 
  //Override
  public Set<String> getPoolNames() {
    Set<String> names = new HashSet<String>();
    for (CassandraClientPoolByHost pool: pools.values()) {
      names.add(pool.getName());
    }
    return names;
  }
 
  //Override
  public void releaseClient(ICassandraClient client) {
    getPool(client).releaseClient(client);
  }
 
  //Override
  public void updateKnownHosts() {
    for (CassandraClientPoolByHost pool: pools.values()) {
      pool.updateKnownHosts();
    }
  }
 
  //Override
  public Set<String> getKnownHosts() {
    Set<String> hosts = new HashSet<String>();
    for (CassandraClientPoolByHost pool: pools.values()) {
      hosts.addAll(pool.getKnownHosts());
    }
    return hosts;
  }
 
  private class PoolKey {
    //@SuppressWarnings("unused")
    private sealed String url, ip;
    //@SuppressWarnings("unused")
    private sealed int port;
    private sealed String name;
 
    public PoolKey(String url, int port) {
      this.port = port;
      StringBuilder b = new StringBuilder();
      InetAddress address;
      String turl, tip;
      try {
        address = InetAddress.getByName(url);
        //turl = address.getHostName();
        tip = address.getHostAddress();
      } catch (Exception e) {
        //log.error("Unable to resolve host {}", url);
        turl = url;
        tip = url;
      }
      this.url = url;
      ip = tip;
      b.Append(url);
      b.Append("(");
      b.Append(ip);
      b.Append("):");
      b.Append(port);
      name = b.ToString();
    }
 
    //Override
    public String toString() {
      return name;
    }
 
    //Override
    public boolean equals(Object obj) {
      if (!(obj InstanceOf PoolKey)) {
        return false;
      }
      return ((PoolKey) obj).name.Equals(name);
    }
 
    //Override
    public int hashCode() {
      return name.GetHashCode();
    }
 
  }
 
  //Override
  public void invalidateClient(ICassandraClient client) {
    getPool(client).invalidateClient(client);
 
  }
 
  void reportDestroyed(ICassandraClient client) {
    ((CassandraClientPoolByHostImpl) getPool(client)).reportDestroyed(client);
  }
 
  private CassandraClientPoolByHost getPool(ICassandraClient c) {
    return getPool(c.getUrl(), c.getPort());
  }
 
  //Override
  public void releaseKeyspace(IKeyspace k) {
    releaseClient(k.getClient());
  }
 
  //Override
  public ICassandraClient borrowClient(String urlPort) {
    int delim = urlPort.LastIndexOf(':');
    String url = urlPort.Substring(0, delim);
    String strPort = urlPort.Substring(delim + 1, urlPort.Length);
    int port = int.Parse(strPort);
    return borrowClient(url, port);
  }
 
  //Override
  public ICassandraClient borrowClient(String[] clientUrls) {
    List<String> clients = new List<String>(clientUrls);
    while(!clients.isEmpty()) {
      Random random = new Random();
      int rand = (int) (random.Next(clients.Count()));
      try {
        return borrowClient(clients[rand]);
      } catch (Exception e) {
        if (clients.Count() > 1) {
          //log.warn("Unable to obtain client " + clients.get(rand) + " will try the next client", e);
          clientMonitor.incCounter(Counter.RECOVERABLE_LB_CONNECT_ERRORS);
          clients.RemoveAt(rand);
        } else {
          throw e;
        }
      }
    }
    // Method should never get here; an exception must have been thrown before, I'm only writing
    // this to make the compiler happy.
    return null;
  }
}
}
