using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra060
{
	public partial class InvalidRequestException
	{
		public InvalidRequestException(string message)
			: base(message)
		{ }
	}
}
