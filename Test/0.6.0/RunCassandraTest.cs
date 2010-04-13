using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using System.Threading;

namespace HectorSharp.Test._060
{
	public class RunCassandraTest
	{
		[Fact]//(Skip="quick integration test, don't run with other tests")]
		public void RunCassandra()
		{
			CassandraRunner.Start();
			Thread.Sleep(2000);
			Assert.True(CassandraRunner.Running);
			CassandraRunner.Stop();
			Assert.False(CassandraRunner.Running);
		}
	}
}
