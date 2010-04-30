using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp
{
	public class UnavailableException : ApplicationException
	{
		public UnavailableException()
		{ }

		public UnavailableException(string message)
			: base(message)
		{ }

		public UnavailableException(string message, Exception inner)
			: base(message, inner)
		{ }
	}
}
