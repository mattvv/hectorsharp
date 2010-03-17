using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Utils
{
	public static class Util
	{
		static readonly DateTime Epoch = new DateTime(1970, 1, 1);
	
		public static long UnixTimestamp
		{ 
			get { return (long)(DateTime.UtcNow - Epoch).TotalMilliseconds; }
		}
	}
}
