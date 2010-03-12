using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Service
{
	public class CassandraClientConfig
	{
		public LoadBalancingStrategy LoadBalancingStrategy { get; set; }
		public IList<Endpoint> Hosts { get; set; }
	}
}
