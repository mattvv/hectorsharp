using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using Thrift;

namespace HectorSharp
{
	/// <summary>
	/// Client object, a high level handle to the remove cassandra service.
	/// From a client you can obtain a Keyspace. A keyspace lets you write/read the remote cassandra.
	/// </summary>
	/// <remarks>
	/// The client is not thread safe, do not share it between threads!
	/// </remarks>
	public interface ICassandraClient
	{
		int Port { get; }

		ICassandraClientMonitor Monitor { get; }

		/**
		 * @return the underline cassandra thrift object, all remote calls will be sent to this client.
		 */
		Apache.Cassandra.Cassandra.Iface Client { get; }
		/// <summary>
		/// Return given key space, if keySpaceName not exist, will throw an exception.
		/// Thread safety: not safe ;-)
		/// Really, if you require thread safety do it at the application level, this class does not provide it.
		/// Uses the default consistency level, {@link #DEFAULT_CONSISTENCY_LEVEL}
		/// Uses the default failover policy {@link #DEFAULT_FAILOVER_POLICY}
		/// </summary>
		/// <param name="keyspaceName"></param>
		/// <returns></returns>
		IKeyspace GetKeyspace(String keyspaceName);
		/// <summary>
		/// Gets s keyspace with the specified consistency level.
		/// </summary>
		/// <param name="keyspaceName"></param>
		/// <param name="consistencyLevel"></param>
		/// <param name="failoverPolicy"></param>
		/// <returns></returns>
		IKeyspace GetKeyspace(string keyspaceName, ConsistencyLevel consistencyLevel, FailoverPolicy failoverPolicy);
		/// <summary>
		/// Gets a string property from the server, such as:
		/// "cluster name": cluster name;
		/// "config file" : all config file content, if need you can try to explain it.
		/// "token map" :  get the token map from local gossip protocal.
		/// </summary>
		/// <param name="propertyName"></param>
		/// <returns></returns>
		string GetStringProperty(string propertyName);
		IList<string> Keyspaces { get; }
		void RemoveKeyspace(IKeyspace k);
		string ClusterName { get; }
		/// <summary>
		/// Gets the token map with an option to refresh the value from cassandra.
		/// If fresh is false, a local cached value may be returned.
		/// </summary>
		/// <param name="fresh">
		/// Whether to query cassandra remote host for an up to date value, or to serve
		/// a possibly cached value.
		/// </param>
		/// <returns></returns>
		IDictionary<string, string> GetTokenMap(bool fresh);
		string ConfigFile { get; }
		string ServerVersion { get; }
		/// <summary>
		/// Tells all instanciated keyspaces to update their known hosts
		/// </summary>
		void MarkAsClosed();
		bool IsClosed { get; }
		IList<Endpoint> KnownEndpoints { get; }
		void UpdateKnownEndpoints();
		Endpoint Endpoint { get; }
		void MarkAsError();
		bool HasErrors { get; }
	}
}