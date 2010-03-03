using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using HectorSharp.Service;

namespace HectorSharp.Dao
{
/**
 * An example DAO (data access object) which uses the Command pattern.
 *
 * This DAO is simple, it provides a get/insert/delte API for String values.
 * The underlying cassandra implementation stores the values under Keyspace1.key.Standard1.v
 * where key is the value's key, Standard1 is the name of the column family and "v" is just a column
 * name that's used to hold the value.
 * <p>
 * what's interesting to notice here is that ease of operation that the command pattern provides.
 * The pattern assumes only one keyspace is required to perform the operation (get/insert/remove)
 * and injects it to the {@link Command#execute(Keyspace)} abstract method which is implemented
 * by all the dao methods.
 * The {@link Command#execute(String, int, String)} which is then invoked, takes care of creating
 * the {@link Keyspace} instance and releasing it after the operation completes.
 *
 * @author Matt Van Veenendaal (m@mattvv.com)
 * @author Ran Tavory (rantav@gmail.com) (Original Java Version)
 *
 */
public class ExampleDao {
 
  private sealed static String CASSANDRA_KEYSPACE = "Keyspace1";
  private sealed static int CASSANDRA_PORT = 9170;
  private sealed static String CASSANDRA_HOST = "localhost";
  private sealed String CF_NAME = "Standard1";
  /** Column name where values are stored */
  private sealed String COLUMN_NAME = "v";
 
  /**
   * Insert a new value keyed by key
   * @param key Key for the value
   * @param value the String value to insert
   */
  public void insert(String key, String value) {
    execute(new Command<Void>(){
      public Void execute(Keyspace ks) {
        ks.insert(key, createColumnPath(COLUMN_NAME), bytes(value));
        return null;
      }
    });
  }
 
  /**
   * Get a string value.
   * @return The string value; null if no value exists for the given key.
   */
  public String get(String key) {
    return execute(new Command<String>(){
      public String execute(Keyspace ks) {
        try {
          return string(ks.getColumn(key, createColumnPath(COLUMN_NAME)).getValue());
        } catch (NotFoundException e) {
          return null;
        }
      }
    });
  }
 
  /**
   * Delete a key from cassandra
   */
  public void delete(String key) {
    execute(new Command<Void>(){
      public Void execute(Keyspace ks) {
        ks.remove(key, createColumnPath(COLUMN_NAME));
        return null;
      }
    });
  }
 
  protected static <T> T execute(Command<T> command) {
    return command.execute(CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE);
  }
 
  protected ColumnPath createColumnPath(String columnName) {
    return new ColumnPath(CF_NAME , null /*superColumn*/, bytes(columnName));
  }
}
}
