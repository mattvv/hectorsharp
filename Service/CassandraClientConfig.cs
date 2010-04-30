using System.Collections.Generic;

namespace HectorSharp
{
	public class CassandraClientConfig
	{
		public LoadBalancingStrategy LoadBalancingStrategy { get; set; }
		public IList<Endpoint> Hosts { get; set; }
	}
}
