using HectorSharp.Model;
using HectorSharp.Utils.ObjectPool;

namespace HectorSharp.Service
{
	public class CassandraClientPoolFactory : IKeyedObjectPoolFactory<Endpoint, ICassandraClient>
	{
		static IKeyedObjectPool<Endpoint, ICassandraClient> pool;

		#region IKeyedObjectPoolFactory<Endpoint,CassandraClient> Members
		
		public IKeyedObjectPool<Endpoint, ICassandraClient> Create()
		{
			return Create(new KeyedCassandraClientFactory(null, null));
		}

		public IKeyedObjectPool<Endpoint, ICassandraClient> Create(IKeyedPoolableObjectFactory<Endpoint, ICassandraClient> factory)
		{
			if (pool == null)
			{
				pool = new KeyedObjectPool<Endpoint, ICassandraClient>(null,
					new KeyedObjectPool<Endpoint, ICassandraClient>.Configuration
					{
						MaxSize = 25,
						MinSize = 4,
						Timeout = 20
					});
				pool.SetFactory(factory);
			}
			return pool;
		}
		#endregion
	}
}
