using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using HectorSharp.Service;
using Thrift;

namespace HectorSharp.Service
{
	public enum FailoverPolicy
	{

		// On communication failure, just return the error to the client and don't retry
		FAIL_FAST,
		// On communication error try one more server before giving up
		ON_FAIL_TRY_ONE_NEXT_AVAILABLE,
		// On communication error try all known servers before giving up 
		ON_FAIL_TRY_ALL_AVAILABLE
	}

	/**
	 * Client object, a high level handle to the remove cassandra service.
	 * <p>
	 * From a client you can obtain a Keyspace. A keyspace lets you write/read the remote cassandra.
	 * <p>
	 * Thread safely: The client is not thread safe, do not share it between threads!
	 *
	 * @author mattvv
	 * @author rantav (Original Java Version)
	 */


	public interface ICassandraClient
	{


		/**
		 * What should the client do if a call to cassandra node fails and we suspect that the node is
		 * down. (e.g. it's a communication error, not an application error).
		 *
		 * {@value #FAIL_FAST} will return the error as is to the user and not try anything smart
		 *
		 * {@value #ON_FAIL_TRY_ONE_NEXT_AVAILABLE} will try one more random server before returning to the
		 * user with an error
		 *
		 * {@value #ON_FAIL_TRY_ALL_AVAILABLE} will try all available servers in the cluster before giving
		 * up and returning the communication error to the user.
		 *
		 */

		/*
			private sealed int numRetries;
 
			FailoverPolicy(int numRetries) {
			  this.numRetries = numRetries;
			}
 
			public int getNumRetries() {
			  return numRetries;
			}
		 }*/


		/**
		 * @return the underline cassandra thrift object, all remote calls will be sent to this client.
		 */
		Cassandra.Client getCassandra();

		/**
		 * Return given key space, if keySpaceName not exist, will throw an exception.
		 * <p>
		 * Thread safety: not safe ;-)
		 * Really, if you require thread safety do it at the application level, this class does not
		 * provide it.
		 * <p>
		 * Uses the default consistency level, {@link #DEFAULT_CONSISTENCY_LEVEL}
		 * <p>
		 * Uses the default failover policy {@link #DEFAULT_FAILOVER_POLICY}
		 */
		IKeyspace getKeyspace(String keyspaceName);


		/**
		 * Gets s keyspace with the specified consistency level.
		 */
		IKeyspace getKeyspace(String keyspaceName, int consistencyLevel, FailoverPolicy failoverPolicy);


		/**
		 * Gets a string property from the server, such as:
		 * "cluster name": cluster name;
		 * "config file" : all config file content, if need you can try to explain it.
		 * "token map" :  get the token map from local gossip protocal.
		 */
		String getStringProperty(String propertyName);


		/**
		 * @return all keyspaces name of this client.
		 */
		List<String> getKeyspaces();


		/**
		 * @return target server cluster name
		 */
		String getClusterName();

		/**
		 * Gets the token map with an option to refresh the value from cassandra.
		 * If fresh is false, a local cached value may be returned.
		 *
		 * @param fresh Whether to query cassandra remote host for an up to date value, or to serve
		 *  a possibly cached value.
		 * @return  a map from tokens to hosts.
		 */
		Dictionary<String, String> getTokenMap(bool fresh);


		/**
		 * @return config file content.
		 */
		String getConfigFile();

		/**
		 * @return Server version
		 */
		String getServerVersion();

		public int getPort();

		public String getUrl();

		/**
		 * Tells all instanciated keyspaces to update their known hosts
		 */
		void updateKnownHosts();

		void markAsClosed();

		bool isClosed();

		List<String> getKnownHosts();

		String getIp();

		void markAsError();

		bool hasErrors();

		void removeKeyspace(IKeyspace k);

	}
}
