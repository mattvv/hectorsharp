using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp
{
	internal static class Util
	{
		static readonly DateTime Epoch = new DateTime(1970, 1, 1);
	
		public static long UnixTimestamp
		{ 
			get { return Convert.ToInt64((DateTime.UtcNow - Epoch).TotalMilliseconds);  }
		}

		public static IEnumerable<TOutput> Transform<TInput, TOutput>(this IEnumerable<TInput> input, Func<TInput, TOutput> transform)
		{
			if (input != null && transform != null)
				foreach (var item in input) yield return transform(item);
		}

		public static bool IsNotNullOrEmpty<T>(this ICollection<T> instance)
		{
			return instance != null && instance.Count > 0;
		}
	}
}
