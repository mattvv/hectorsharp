using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp;
using Apache.Cassandra;
using Thrift.Protocol;
using Thrift.Transport;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Service
{
    class CassandraClientFactory : IPoolableObjectFactory<CassandraClient>
    {
        CassandraClientMonitor monitor;

        IObjectPool<CassandraClient> pool;
        Endpoint endpoint;

        int Timeout { get { return 10; } }

        public CassandraClientFactory(IObjectPool<CassandraClient> pool, Endpoint endpoint, CassandraClientMonitor monitor)
        {
            this.pool = pool;
            this.endpoint = endpoint;
            this.monitor = monitor;
        }

        #region IPoolableObjectFactory<CassandraClient> Members

        public CassandraClient Make()
        {
            TTransport transport = new TSocket(endpoint.Host, endpoint.Port, Timeout);
            TProtocol protocol = new TBinaryProtocol(transport);
            Cassandra.Client thriftClient = new Cassandra.Client(protocol);

            try
            {
                transport.Open();
            }
            catch (TTransportException e)
            {
                // Thrift exceptions aren't very good in reporting, so we have to catch the exception here and
                // add details to it.
                throw new Exception("Unable to open transport to " + endpoint.ToString() + " , ", e);
            }

            return new CassandraClient(thriftClient,
                new KeyspaceFactory(monitor),
                host, port, pool);
        }

        public bool Destroy(CassandraClient client)
        {
            // ((CassandraClientPoolImpl) pool).reportDestroyed(cclient);
            try
            {
                Cassandra.Client thriftClient = client.Client;
                thriftClient.InputProtocol.Transport.Close();
                thriftClient.OutputProtocol.Transport.Close();
                client.markAsClosed();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public void Activate(CassandraClient obj)
        { /* no-op */ }

        public void Passivate(CassandraClient obj)
        { /* no-op */ }

        public bool Validate(CassandraClient client)
        {
            return !client.IsClosed && !client.HasErrors;
        }

        #endregion
    }
}
