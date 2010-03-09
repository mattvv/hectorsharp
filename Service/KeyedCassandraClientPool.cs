using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Utils.ObjectPool;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;

namespace HectorSharp.Service
{
	class KeyedCassandraClientPool : IKeyedObjectPool<Endpoint, CassandraClient>
	{
		Dictionary<Endpoint, HostPool> pools = new Dictionary<Endpoint, HostPool>();

		class HostPool
		{
			public HostPool(Endpoint endpoint, KeyedCassandraClientFactory factory)
			{
				Endpoint = endpoint;
				Pool = new ObjectPool<CassandraClient>(null, new ObjectPool<CassandraClient>.Configuration
				{
					MaxSize = 20,
					MinSize = 4,
					Timeout = 15,
				});
				var fac = new CassandraClientFactory(Pool, endpoint, new CassandraClientMonitor());
				Pool.SetFactory(fac);
			}
			public Endpoint Endpoint { get; private set; }
			public ObjectPool<CassandraClient> Pool { get; private set; }
			public int ActiveCount { get { return Pool.Active; } }
			public int BorrowCount { get { return Pool.BorrowCount; } }
		}

		public ObjectPool<CassandraClient> GetPoolByEndpoint(Endpoint key)
		{
			if (key == null || !pools.ContainsKey(key))
				return null;

			return pools[key].Pool;
		}

		#region IKeyedObjectPool<string,CassandraClient> Members

		public int GetActiveCount()
		{
			return pools.Sum(p => p.Value.ActiveCount);
		}

		public int GetActiveCount(Endpoint key)
		{
			return pools[key].ActiveCount;
		}

		public int GetIdleCount()
		{
			return pools.Sum(p => p.Value.Pool.Idle);
		}

		public int GetIdleCount(Endpoint key)
		{
			return pools[key].Pool.Idle;
		}

		public CassandraClient Borrow(Endpoint key)
		{
			return pools[key].Pool.Borrow();
		}

		public void Return(Endpoint key, CassandraClient obj)
		{
			pools[key].Pool.Return(obj);
		}

		public void Add(Endpoint key)
		{
			if (!pools.ContainsKey(key))
				pools.Add(key, new HostPool(key));
		}

		public void Clear()
		{
			foreach (var pool in pools)
				pool.Value.Pool.Clear();
		}

		public void Clear(Endpoint key)
		{
			if (pools.ContainsKey(key))
				pools[key].Pool.Clear();
		}

		public void Close()
		{
			foreach (var pool in pools)
				pool.Value.Pool.Close();
		}

		public void SetFactory(IKeyedPoolableObjectFactory<Endpoint, CassandraClient> factory)
		{
			/* no-op */
		}

		#endregion
	}
}