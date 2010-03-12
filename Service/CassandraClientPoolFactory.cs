using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Service
{
	public class CassandraClientPoolFactory : IKeyedObjectPoolFactory<Endpoint, ICassandraClient>
	{
		static IKeyedObjectPool<Endpoint, ICassandraClient> pool;

		#region IKeyedObjectPoolFactory<Endpoint,CassandraClient> Members

		public IKeyedObjectPool<Endpoint, ICassandraClient> Create()
		{
			if (pool == null)
			{
				pool = new KeyedObjectPool<Endpoint, ICassandraClient>(null,
					new KeyedObjectPool<Endpoint, ICassandraClient>.Configuration
					{
						MaxSize = 25, MinSize = 4, Timeout = 20
					});
				var factory = new KeyedCassandraClientFactory(null, null);
				pool.SetFactory(factory);
			}
			return pool;
		}

		#endregion
	}
}
