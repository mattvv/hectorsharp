using System;
using System.Net;

namespace HectorSharp
{
	public class InetAddress : IComparable, IComparable<InetAddress>
	{
		IPAddress addr;
		string host;

		public InetAddress(string addr)
		{
			IPAddress ip;
			if (IPAddress.TryParse(addr, out ip))
				this.addr = ip;
			else if (TryResolveInetAddress(addr, out ip, out this.host))
				this.addr = ip;
			else
				throw new ArgumentException("InetAddress: unable to parse or resolve [" + addr + "]");
		}

		public InetAddress(IPAddress addr)
		{
			this.addr = addr;
		}

		public bool IsAnyLocalAddress()
		{
			return IPAddress.IsLoopback(addr);
		}

		public override bool Equals(object obj)
		{
			if (obj == null || GetType() != obj.GetType())
				return false;

			return equals(obj.ToString());
		}

		bool equals(InetAddress addr)
		{
			return addr.ToString().Equals(addr.ToString());
		}

		bool equals(string addr)
		{
			return addr.ToString().Equals(addr.ToString());
		}

		public override string ToString()
		{
			return addr.ToString();
		}

		public string GetHostAddress()
		{
			return ToString();
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		public static InetAddress GetByHostName(string name)
		{
			IPAddress ip;
			string hostname;
			if (!TryResolveInetAddress(name, out ip, out hostname))
				throw new ApplicationException(String.Format("Unable to resolve host: [{0}]", name));

			return new InetAddress(ip);
		}

		static bool TryResolveInetAddress(string addr, out IPAddress ip, out string hostname)
		{
			ip = null;
			hostname = null;

			try
			{
				var result = Dns.GetHostEntry(addr);
				if (result != null && result.AddressList.Length > 0)
				{
					ip = result.AddressList[0];
					hostname = result.HostName;
				}
				return true;
			}
			catch
			{}
			return false;
		}

		#region IComparable Members

		public int CompareTo(object obj)
		{
			return CompareTo((InetAddress)obj);
		}

		public int CompareTo(InetAddress other)
		{
			if (other == null) throw new ArgumentNullException("other");
			return string.Compare(this.ToString(), other.ToString());
		}

		#endregion
	}
}