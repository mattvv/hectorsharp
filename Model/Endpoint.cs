using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp
{
	public class Endpoint : IComparable<Endpoint>, IComparable<string>, IComparable
	{
		string key;

		public Endpoint(string host, int port, string ip)
		{
			if (string.IsNullOrEmpty(host)) throw new ArgumentNullException(host);
			if (port <= 0) throw new ArgumentOutOfRangeException("port");

			Host = host;
			Port = port;
			IP = ip;
			key = String.Format("{0}:{1}", Host, Port);
		}

		public Endpoint(string host, int port)
			: this(host, port, "")
		{}

		public string Host { get; private set; }
		public string IP { get; private set; }
		public int Port { get; private set; }
		public bool IsIPResolved { get { return !string.IsNullOrEmpty(IP); } }

		public void ResolveIP()
		{
			IP = GetIpString(Host);
		}

		static String GetIpString(String host)
		{
			var address = InetAddress.GetByHostName(host);

			return address.GetHostAddress();
		}

		public override string ToString()
		{
			return key;
		}

		#region IComparable<string> Members

		public int CompareTo(string other)
		{
			return string.Compare(key, other.ToString());
		}

		#endregion

		#region IComparable Members

		public int CompareTo(object obj)
		{
			return CompareTo(obj as string);
		}

		#endregion

		#region IComparable<Endpoint> Members

		public int CompareTo(Endpoint other)
		{
			return string.Compare(key, other.key);
		}

		#endregion
	}
}
