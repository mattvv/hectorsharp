using System.Collections.Generic;
using HectorSharp.Model;

namespace HectorSharp.Service
{
	public class CassandraClientConfig
	{
		public LoadBalancingStrategy LoadBalancingStrategy { get; set; }
		public IList<Endpoint> Hosts { get; set; }
	}
}
