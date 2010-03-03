using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp;
using Apache.Cassandra;
using Thrift.Protocol;
using Thrift.Transport;

namespace HectorSharp.Service
{
    class CassandraClientFactory : PoolableObjectFactory
    {
 
  /** Socket timeout */
  private sealed int timeout;
 
  private CassandraClientMonitor clientMonitor;
 
  /**
   * The pool associated with this client factory.
   */
  private sealed CassandraClientPool pool;
  private sealed String url;
  private sealed int port;
 
  public CassandraClientFactory(CassandraClientPool pools, String url, int port,
      CassandraClientMonitor clientMonitor) {
    this.pool = pools;
    this.url = url;
    this.port = port;
    timeout = getTimeout();
    this.clientMonitor = clientMonitor;
  }
 
  public CassandraClient create() {
    return new CassandraClientImpl(createThriftClient(url, port),
        new KeyspaceFactory(clientMonitor), url, port, pool);
  }
 
  private Cassandra.Client createThriftClient(String  url, int port)
       {
    TTransport tr = new TSocket(url, port, timeout);
    TProtocol proto = new TBinaryProtocol(tr);
    Cassandra.Client client = new Cassandra.Client(proto);
    try {
      tr.Open();
    } catch (TTransportException e) {
      // Thrift exceptions aren't very good in reporting, so we have to catch the exception here and
      // add details to it.
      throw new Exception("Unable to open transport to " + url + ":" + port + " , ", e);
    }
    return client;
  }
 
  /**
   * Gets an environment variable CASSANDRA_THRIFT_SOCKET_TIMEOUT value.
   * If doesn't exist, returns 0.
   */
  private int getTimeout() {
    String timeoutStr = Type.GetProperty("CASSANDRA_THRIFT_SOCKET_TIMEOUT");
    if (timeoutStr == null || timeoutStr.Length == 0) {
      return  0;
    } else {
      try {
        return int.Parse(timeoutStr);
      } catch (Exception e) {
        //log.error("Invalid value for CASSANDRA_THRIFT_SOCKET_TIMEOUT", e);
        return 0;
      }
    }
  }
 
  //Override
  public void activateObject(Object obj) {
    // nada
  }
 
  //Override
  public void destroyObject(Object obj) {
    CassandraClient client = (CassandraClient) obj ;
    //log.debug("Close client {}", client);
    closeClient(client);
  }
 
  //Override
  public Object makeObject() {
    //log.debug("Creating a new client...");
    CassandraClient c = create();
    //log.debug("New client created: {}", c);
    return c;
  }
 
  //Override
  public boolean validateObject(Object obj) {
    return validateClient((CassandraClient) obj);
  }
 
  private boolean validateClient(CassandraClient client) {
    // TODO send fast and easy request to cassandra
    return !client.isClosed() && !client.hasErrors();
  }
 
  private void closeClient(CassandraClient cclient) {
    ((CassandraClientPoolImpl) pool).reportDestroyed(cclient);
    Cassandra.Client client = cclient.getCassandra();
    client.InputProtocol.Transport.Close();
    client.OutputProtocol.Transport.Close();
    cclient.markAsClosed();
  }
 
  //Override
  public void passivateObject(Object obj) {
    // TODO Auto-generated method stub
  }
}
