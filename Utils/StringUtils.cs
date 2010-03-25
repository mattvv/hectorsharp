using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Utils
{
	/// <summary>
	/// Encoding and decoding utilities.
	/// </summary>
	public static class StringUtils
	{
		//private static sealed Logger log = LoggerFactory.getLogger(StringUtils.class);

		static ASCIIEncoding ascii = new ASCIIEncoding();
		static UTF8Encoding utf8 = new UTF8Encoding();

		public static byte[] ToAsciiBytes(this string instance)
		{
			if (String.IsNullOrEmpty(instance))
				return new byte[0];

			return ascii.GetBytes(instance);
		}

		public static string DecodeAsciiString(this byte[] instance)
		{
			if (instance.Length == 0)
				return string.Empty;

			return ascii.GetString(instance);
		}

		public static byte[] UTF(this string instance)
		{
			return instance.ToUtf8Bytes();
		}

		public static byte[] ToUtf8Bytes(this string instance)
		{
			if (String.IsNullOrEmpty(instance))
				return null; // new byte[0];

			return utf8.GetBytes(instance);
		}

		public static string DecodeUtf8String(this byte[] instance)
		{
			if (instance.Length == 0)
				return string.Empty;

			return utf8.GetString(instance);
		}

		public static string UTFDecode(this byte[] instance)
		{
			return instance.DecodeUtf8String();
		}
	}
}
