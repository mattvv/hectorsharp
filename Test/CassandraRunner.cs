using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.IO;
using Xunit;

namespace HectorSharp.Test
{
	static class CassandraRunner
	{
		static Object padlock = new object();
		static Process cassandra = null;
		static string originalWorkingDirectory = Environment.CurrentDirectory;
		static string cassandraHome = Environment.GetEnvironmentVariable("CASSANDRA_HOME");
		static string comspec = Environment.GetEnvironmentVariable("ComSpec");
		static string debugDir = "Test/bin/Debug/";

		static DirectoryInfo rundir;

		public static bool Running { get; private set; }
		public static int ExitCode { get; private set; }

		static CassandraRunner()
		{
			// if running from msbuild for CI server will be ./HectorSharp
			originalWorkingDirectory = Environment.CurrentDirectory;
			var curdir = new DirectoryInfo(Environment.CurrentDirectory);

			if (!Directory.Exists(cassandraHome))
				throw new ApplicationException("unable to find path specified in CASSANDRA_HOME environment variable: " + cassandraHome);

			bool isDebug = curdir.Name.Equals("debug", StringComparison.InvariantCultureIgnoreCase);
			bool isRelease = curdir.Name.Equals("release", StringComparison.InvariantCultureIgnoreCase);

			if (isDebug || isRelease)
				rundir = curdir; // assumes cwd = ./HectorSharp/Test/bin/Debug
			else if (Directory.Exists(debugDir))
				rundir = new DirectoryInfo(debugDir);
			else
				throw new ApplicationException("unable to resolve path to ./HectorSharp/" + debugDir);
		}

		public static void Start()
		{
			if (Running) return;

			lock (padlock)
			{
				var reset = new AutoResetEvent(false);
				cassandra = RunBatchFile("RunCassandra.bat", false);
				cassandra.Exited += new EventHandler((sender, e) => { Console.WriteLine("CASSANDRA EXITED!"); });
				cassandra.OutputDataReceived += new DataReceivedEventHandler(
					(sender, e) =>
					{
						reset.Set();
						Console.WriteLine("CASSANDRA > " + e.Data);
					});
				cassandra.ErrorDataReceived += new DataReceivedEventHandler(
					(sender, e) =>
					{
						reset.Set();
						Console.WriteLine("CASSANDRA ERROR > " + e.Data);
					});
				cassandra.EnableRaisingEvents = true;

				cassandra.Start();
				
				cassandra.BeginOutputReadLine();
				cassandra.BeginErrorReadLine();

				reset.WaitOne(3000); // wait up to 3 seconds before continuing
				Console.WriteLine("Cassandra, pid: {0} is active: {1}", cassandra.Id, !cassandra.HasExited);

				if (!cassandra.HasExited)
					Running = true;
			}
		}

		static void cassandra_Exited(object sender, EventArgs e)
		{
			throw new NotImplementedException();
		}

		static void cassandra_OutputDataReceived(object sender, DataReceivedEventArgs e)
		{
			throw new NotImplementedException();
		}

		public static void Stop()
		{
			lock (padlock)
			{
				if (!Running) return;

				// send control-c to cancel the batch file
				cassandra.StandardInput.Write("\x3");
				// terminate batch file? (y/n)
				cassandra.StandardInput.WriteLine("y");
				cassandra.WaitForExit(1000);
				Console.WriteLine("Cassandra, pid: {0} is active: {1}", cassandra.Id, !cassandra.HasExited);


				if (cassandra.HasExited)
				{
					ExitCode = cassandra.ExitCode;
					cassandra.Close();
				}
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

			var process = RunBatchFile("CleanCassandraData.bat", true);
			process.WaitForExit(2000);
			if (!process.HasExited)
				process.Kill();
		}

		static Process RunBatchFile(string filename, bool start)
		{
			if (!Environment.CurrentDirectory.Equals(rundir.FullName))
				Environment.CurrentDirectory = rundir.FullName;

			var bat = rundir.GetFiles(filename)[0];

			var startInfo = new ProcessStartInfo(comspec, "/C " + bat.FullName)
			{
				WindowStyle = ProcessWindowStyle.Hidden,
				UseShellExecute = false,
				CreateNoWindow = true,
				RedirectStandardInput = true,
				RedirectStandardOutput = true,
				RedirectStandardError = true,
			};
			var process = new Process { StartInfo = startInfo };
			Console.WriteLine("Running Batch File: " + bat.FullName);
			if (start)
				process.Start();
			return process;
		}
	}
}