using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Cassandra
{
	public partial class InvalidRequestException
	{
		public InvalidRequestException(string message)
			: base(message)
		{ }
	}
}
