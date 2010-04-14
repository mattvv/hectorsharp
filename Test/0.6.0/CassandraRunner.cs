using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.IO;
using Xunit;

namespace HectorSharp.Test._060
{
	static class CassandraRunner
	{
		static Object padlock = new object();
		static Process cassandra = null;
		static string originalWorkingDirectory = Environment.CurrentDirectory;
		static string batchDir = "0.6.0";
		static string comspec = Environment.GetEnvironmentVariable("ComSpec");
		static DirectoryInfo rundir;

		public static bool Running { get; private set; }
		public static int ExitCode { get; private set; }

		static CassandraRunner()
		{
			// if running from msbuild for CI server will be ./HectorSharp
			originalWorkingDirectory = Environment.CurrentDirectory;
			var curdir = new DirectoryInfo(Environment.CurrentDirectory);
			if (curdir.Name.Equals("debug", StringComparison.InvariantCultureIgnoreCase))
				// assumes cwd = ./HectorSharp/Test/bin/Debug
				// set to ../../0.6.0
				rundir = curdir.Parent.GetDirectories(batchDir)[0];
			else if (Directory.Exists("Test/" + batchDir))
				rundir = new DirectoryInfo("Test/" + batchDir);
			else
				throw new ApplicationException("unable to resolve path to ./HectorSharp/Test/" + batchDir);
		}

		public static void Start()
		{
			if (Running) return;

			lock (padlock)
			{
				cassandra = RunBatchFile("RunCassandra.bat");
				Console.WriteLine("Waiting 3 seconds while Cassandra starts up");
				Console.WriteLine("Cassandra, pid: {0} is active: {1}", cassandra.Id, !cassandra.HasExited);
				Console.WriteLine(cassandra.StandardOutput.ReadToEnd());
				//Thread.Sleep(3000);

				if (!cassandra.HasExited)
					Running = true;
			}
		}

		public static void Stop()
		{
			lock (padlock)
			{
				if(!Running) return;

				// send control-c to cancel the batch file
				cassandra.StandardInput.Write("\x3");
				// terminate batch file? (y/n)
				cassandra.StandardInput.WriteLine("y");
				cassandra.Close();
				cassandra.WaitForExit(1000);

				Console.WriteLine("Cassandra, pid: {0} is active: {1}", cassandra.Id, !cassandra.HasExited);
				
				if(cassandra.HasExited)
					ExitCode = cassandra.ExitCode;
				else
					cassandra.Kill();

				Running = false;
				cassandra = null;
				Environment.CurrentDirectory = originalWorkingDirectory;
			}
		}

		public static void CleanData()
		{
			if (Running) return;

			var process = RunBatchFile("CleanCassandraData.bat");
			process.WaitForExit(2000);
			if (!process.HasExited)
				process.Kill();
		}

		static Process RunBatchFile(string filename)
		{
			if(!Environment.CurrentDirectory.Equals(rundir.FullName))
				Environment.CurrentDirectory = rundir.FullName;
		
			var bat = rundir.GetFiles(filename)[0];
			Console.WriteLine("Running Batch File: " + bat.FullName);

			return Process.Start(new ProcessStartInfo(comspec, "/C " + bat.FullName)
			{
				WindowStyle = ProcessWindowStyle.Hidden,
				UseShellExecute = false,
				CreateNoWindow = true,
				RedirectStandardInput = true,
				RedirectStandardOutput = true,
				RedirectStandardError = true,
			});
		}
	}
}