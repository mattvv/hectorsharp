using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp
{
	public class NotFoundException : ApplicationException
	{
		public NotFoundException()
		{}

		public NotFoundException(string message)
			: base(message)
		{}

		public NotFoundException(string message, Exception inner)
			:base(message, inner)
		{}
	}
}
