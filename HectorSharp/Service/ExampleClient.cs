using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using HectorSharp.Utils;

namespace HectorSharp.Service
{
/**
 * Example client that uses the cassandra hector client.
 *
 * @author Matt Van Veenendaal (m@mattvv.com)
 * @author Ran Tavory (rantav@gmail.com) (original java)
 *
 */
public class ExampleClient {
 
  public static void main(String[] args) {
    CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
    CassandraClient client = pool.borrowClient("192.168.2.17", 9160);
    try {
      Keyspace keyspace = client.getKeyspace("Keyspace1");
      ColumnPath columnPath = new ColumnPath("Standard1", null, StringUtils.bytes("column-name"));
 
      // insert
      keyspace.insert("key", columnPath, StringUtils.bytes("value"));
 
      // read
      Column col = keyspace.getColumn("key", columnPath);
 
      Console.Out.WriteLine("Read from cassandra: " + col.ToString());
 
    } finally {
      // return client to pool. do it in a finally block to make sure it's executed
      pool.releaseClient(client);
    }
  }
}
}
