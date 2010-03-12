using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace HectorSharp.Test
{
	/// <summary>
	/// Summary description for CassandraClientTest
	/// </summary>
	[TestClass]
	public class CassandraClientTest
	{
		private CassandraClientPool pool;


		public TestContext TestContext { get; set; }

		[ClassInitialize]
		public static void ClassInitialize(TestContext context)
		{
		}

		[ClassCleanup]
		public static void ClassCleanup()
		{
		}

		[TestInitialize]
		public void TestInitialize()
		{
		}



		[TestMethod]
		public void TestMethod1()
		{
			//
			// TODO: Add test logic	here
			//
		}
	}
}
