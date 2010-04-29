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
		static DirectoryInfo cassandraHomeDir;
		static string javaHome = Environment.GetEnvironmentVariable("JAVA_HOME");
		static DirectoryInfo javaHomeDir;
		static string comspec = Environment.GetEnvironmentVariable("ComSpec");
#if DOTNET35
		static string debugDir = "Test/bin/dotnet35/Debug/";
#else
		static string debugDir = "Test/bin/Debug/";
#endif
		static DirectoryInfo rundir;

		public static bool Running { get; private set; }
		public static int ExitCode { get; private set; }

		static CassandraRunner()
		{
			// if running from msbuild for CI server will be ./HectorSharp
			originalWorkingDirectory = Environment.CurrentDirectory;
			var curdir = new DirectoryInfo(Environment.CurrentDirectory);

			if (!Directory.Exists(javaHome))
				throw new ApplicationException("unable to find path specified in JAVA_HOME environment variable: " + javaHome);

			javaHomeDir = new DirectoryInfo(javaHome);

			if (!Directory.Exists(cassandraHome))
				throw new ApplicationException("unable to find path specified in CASSANDRA_HOME environment variable: " + cassandraHome);

			cassandraHomeDir = new DirectoryInfo(cassandraHome);

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
				var bat = RunBatchFile("SetupCassandraEnvironment.bat", true);
				bat.WaitForExit(500);
				if (!bat.HasExited)
					bat.Kill();
				bat.Close();

				if (!Environment.CurrentDirectory.Equals(cassandraHomeDir.FullName))
					Environment.CurrentDirectory = cassandraHomeDir.FullName;

				var java_exe = javaHomeDir.GetFiles("bin/java.exe")[0].FullName;

				var args = BuildArgs();

				var startInfo = new ProcessStartInfo(java_exe, args)
				{
					WindowStyle = ProcessWindowStyle.Hidden,
					UseShellExecute = false,
					CreateNoWindow = true,
					RedirectStandardInput = true,
					RedirectStandardOutput = true,
					RedirectStandardError = true,
				};
				startInfo.EnvironmentVariables.Add("CASSANDRA_CONF", @"V:\conf");
				cassandra = new Process
				{
					StartInfo = startInfo,
					EnableRaisingEvents = true,
				};

				var reset = new AutoResetEvent(false);

				cassandra.Exited += new EventHandler((sender, e) => { Console.WriteLine("CASSANDRA EXITED!"); });
				cassandra.OutputDataReceived += new DataReceivedEventHandler((sender, e) =>
				{
					if (!string.IsNullOrEmpty(e.Data))
					{
						Console.WriteLine("CASSANDRA > " + e.Data);

						if(e.Data.Contains("Cassandra starting up..."))
							reset.Set();
					}
				});
				cassandra.ErrorDataReceived += new DataReceivedEventHandler((sender, e) =>
				{
					if(!string.IsNullOrEmpty(e.Data))
						Console.WriteLine("CASSANDRA ERROR > " + e.Data);
				});

				cassandra.Start();
				cassandra.BeginOutputReadLine();
				cassandra.BeginErrorReadLine();

				reset.WaitOne(5000); // wait up to 5 seconds before continuing
				Console.WriteLine("Cassandra, pid: {0} is active: {1}", cassandra.Id, !cassandra.HasExited);

				if (!cassandra.HasExited)
					Running = true;
			}
		}

		public static void Stop()
		{
			lock (padlock)
			{
				if (!Running) return;

				// send control-c to cancel the batch file
				//cassandra.StandardInput.Write("\x3");
				// terminate batch file? (y/n)
				//cassandra.StandardInput.WriteLine("y");
				cassandra.WaitForExit(1000);
				Console.WriteLine("Cassandra, pid: {0} is active: {1}", cassandra.Id, !cassandra.HasExited);

				if (cassandra.HasExited)
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

		static string[] java_opts = new string[]
		{
			"-ea",
			"-Xdebug",
			"-Xrunjdwp:transport=dt_socket,server=y,address=8888,suspend=n", 
			"-Xms128m",
			"-Xmx1G", 
			"-XX:TargetSurvivorRatio=90", 
			"-XX:+AggressiveOpts", 
			"-XX:+UseParNewGC", 
			"-XX:+UseConcMarkSweepGC", 
			"-XX:+CMSParallelRemarkEnabled", 
			"-XX:+HeapDumpOnOutOfMemoryError", 
			"-XX:SurvivorRatio=128", 
			"-XX:MaxTenuringThreshold=0", 
			"-Dcom.sun.management.jmxremote.port=8080", 
			"-Dcom.sun.management.jmxremote.ssl=false", 
			"-Dcom.sun.management.jmxremote.authenticate=false", 
			"-Dcassandra", 
			"-Dstorage-config=\"V:\\conf\"", 
			"-Dcassandra-foreground=yes",
		};

		static string BuildClassPath()
		{
			var jars = cassandraHomeDir.GetFiles("lib/*.jar");
			var b = new StringBuilder("-cp \"");
			foreach (var jar in jars)
				b.AppendFormat(";{0}", jar.FullName);
			b.Append("\"");
			return b.ToString();
		}

		static string BuildArgs()
		{
			var b = new StringBuilder();
			foreach (var opt in java_opts)
				b.Append(" ").Append(opt);
			b.Append(" ").Append(BuildClassPath());
			b.Append(" \"").Append("org.apache.cassandra.thrift.CassandraDaemon").Append("\"");
			return b.ToString();
		}
	}
}