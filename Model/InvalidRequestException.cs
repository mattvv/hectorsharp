using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp
{
	public class InvalidRequestException : ApplicationException
	{
		public InvalidRequestException()
		{}

		public InvalidRequestException(string message)
			: base(message)
		{}

		public InvalidRequestException(string message, Exception inner)
			: base(message, inner)
		{}
	}
}
