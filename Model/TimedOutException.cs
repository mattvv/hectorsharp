using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Model
{
	public class TimedOutException : ApplicationException
	{
		public TimedOutException()
		{}

		public TimedOutException(string message)
			: base(message)
		{}

		public TimedOutException(string message, Exception inner)
			: base(message, inner)
		{}
	}
}
