using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Xunit;
using Thrift.Protocol;
using Apache.Cassandra;

namespace Test
{
	using CassandraRunner = HectorSharp.Test.CassandraRunner;

	public class RawThriftFixture : IDisposable
	{
		TTransport transport = new TSocket("localhost", 9060);
		Cassandra.Client client;
		public Cassandra.Client Client { get { return client;} }

		public RawThriftFixture()
		{
			client = new Cassandra.Client(new TBinaryProtocol(transport));
		}

		public void OpenConnection()
		{
			if (!transport.IsOpen)
			{
				Console.WriteLine("Opening Connection");
				transport.Open();
			}
			Assert.True(transport.IsOpen);
		}

		public void CloseConnection()
		{
			if (transport.IsOpen)
			{
				Console.WriteLine("Closing Connection");
				transport.Close();
			}
			Assert.False(transport.IsOpen);
		}

		public void RestartCassandra()
		{
			if (CassandraRunner.Running)
				CassandraRunner.Stop();
			CassandraRunner.CleanData();
			CassandraRunner.Start();
			Assert.True(CassandraRunner.Running);
		}

		public void StopCassandra()
		{
			if (CassandraRunner.Running)
				CassandraRunner.Stop();
			Assert.False(CassandraRunner.Running);
		}

		#region IDisposable Members

		public void Dispose()
		{
			CloseConnection();
			CassandraRunner.Stop();
			CassandraRunner.CleanData();
		}

		#endregion
	}
}
