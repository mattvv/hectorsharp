using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra060
{
	/// <summary>
	/// Encoding and decoding utilities.
	/// </summary>
	internal static class StringUtils
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
				return new byte[0];

			return utf8.GetBytes(instance);
		}

		public static string DecodeUtf8String(this byte[] instance)
		{
			if (instance.Length == 0)
				return string.Empty;

			return utf8.GetString(instance);
		}


		/**
		 * Gets UTF-8 bytes from the string.
		 *
		 * @param s
		 * @return
		 */
		public static byte[] bytes(String s)
		{
			try
			{

				return ascii.GetBytes(s);
			}
			catch (Exception e)
			{
				//log.error("UnsupportedEncodingException ", e);
				throw new Exception(e.Message);
			}
		}

		/**
		 * Utility for converting bytes to strings. UTF-8 is assumed.
		 * @param bytes
		 * @return
		 */
		public static String toString(byte[] bytes)
		{
			if (bytes == null)
			{
				return null;
			}
			try
			{
				return bytes.ToString();
			}
			catch (Exception e)
			{
				//log.error("UnsupportedEncodingException ", e);
				throw new Exception(e.Message);
			}
		}

	}
}
