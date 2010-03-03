using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Service
{
    /**
     *
     * @author Ran Tavory (rantav@gmail.com)
     *
     */
    /*package*/
    class KeyspaceFactory
    {

        private sealed CassandraClientMonitor clientMonitor;

        public KeyspaceFactory(CassandraClientMonitor clientMonitor)
        {
            this.clientMonitor = clientMonitor;
        }

        public Keyspace create(CassandraClient client, String keyspaceName,
            Map<String, Map<String, String>> keyspaceDesc, int consistencyLevel,
            FailoverPolicy failoverPolicy, CassandraClientPool clientPools)
        {
            return new KeyspaceImpl(client, keyspaceName, keyspaceDesc, consistencyLevel,
                failoverPolicy, clientPools, clientMonitor);
        }
    }
}
