using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections;
using System.Collections.Concurrent;

namespace HectorSharp.Utils.ObjectPool
{
	public class KeyedObjectPool<K, V> : IKeyedObjectPool<K, V>
		where K : IComparable
		where V : class
	{
		public class Configuration
		{
			public int MinSize { get; set; }
			public int MaxSize { get; set; }
			public int Timeout { get; set; }
		}

		IKeyedPoolableObjectFactory<K, V> factory;

		public int MinSize { get; private set; }
		public int MaxSize { get; private set; }
		public int Timeout { get; private set; }
		public bool IsClosed { get; private set; }

		IDictionary<K, Queue<V>> idle = new ConcurrentDictionary<K, Queue<V>>();
		IDictionary<K, List<V>> active = new ConcurrentDictionary<K, List<V>>();
		// counters
		IDictionary<K, Counter> available = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> failedActivateIdleCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> failedValidateIdleCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> failedActivateNewCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> failedValidateNewCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> failedValidateReturnCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> failedDestroyCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> failedPassivateReturnCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> reuseCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> borrowedCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> returnCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> makeCounts = new ConcurrentDictionary<K, Counter>();
		IDictionary<K, Counter> failedMakeCounts = new ConcurrentDictionary<K, Counter>();

		AutoResetEvent reset = new AutoResetEvent(false);

		public KeyedObjectPool(IKeyedPoolableObjectFactory<K, V> factory, KeyedObjectPool<K, V>.Configuration config)
		{
			IsClosed = false;
			this.factory = factory;

			if (config != null)
			{
				MinSize = config.MinSize;
				MaxSize = config.MaxSize;
				Timeout = config.Timeout;
			}
			if (MinSize > MaxSize)
				MinSize = MaxSize;

			// populate the idle pool to the minimum size
			reset.Set();
		}

		#region IKeyedObjectPool<K,V> Members

		public IEnumerable<K> Keys
		{
			get
			{
				var keys = new List<K>();
				keys.AddRange(idle.Keys);

				foreach (var key in keys)
					yield return key;
			}
		}

		public bool HasMaxSize { get { return MaxSize > 0; } }

		public int GetActiveCount()
		{
			return active.Sum(a => a.Value.Count);
		}

		public int GetActiveCount(K key)
		{
			return active[key].Count;
		}

		public int GetIdleCount()
		{
			return idle.Sum(i => i.Value.Count);
		}

		public int GetIdleCount(K key)
		{
			return idle[key].Count;
		}

		public V Borrow(K key)
		{
			AssertOpen();

			int fullTimeOut = Timeout * 1000;
			int timeOut = fullTimeOut;
			DateTime start = DateTime.Now;

			while (timeOut > 0)
			{
				var obj = Acquire(key);
				if (obj != null)
				{
					Increment(key, borrowedCounts);
					return obj;
				}

				if (!reset.WaitOne(timeOut, false))
					break;

				if (IsClosed)
					return null;

				timeOut = fullTimeOut - (int)DateTime.Now.Subtract(start).TotalMilliseconds;
			}

			throw new TimeoutException(
				 String.Format(
					  "Exceeded the {0} second timeout while trying to borrow a new \"{1}\" from the pool.",
					  Timeout,
					  typeof(V).Name));

		}

		public void Return(K key, V obj)
		{
			if (!active.ContainsKey(key))
				active.Add(key, new List<V>());

			var activeList = active[key];
			
			lock ((activeList as ICollection).SyncRoot)
			{
				if (activeList.Contains(obj))
					activeList.Remove(obj);
			}

			AssertHasFactory();

			if (!factory.Validate(key, obj))
			{
				Increment(key, failedValidateReturnCounts);

				Destroy(key, obj);
			}
			else
			{
				if (HasMaxSize && idle[key].Count >= MaxSize)
					Destroy(key, obj);
				else
				{
					if (!factory.Passivate(key, obj))
					{
						Increment(key, failedPassivateReturnCounts);
						return;
					}

					if (IsClosed)
					{
						Destroy(key, obj);
						return;
					}

					if (!idle.ContainsKey(key))
						idle.Add(key, new Queue<V>());

					var idleQueue = idle[key];

					lock ((idleQueue as ICollection).SyncRoot)
						idleQueue.Enqueue(obj);
				}
			}

			if (HasMaxSize)
				AvailableIncrement(key);
		
			reset.Set();

			Increment(key, returnCounts);
		}


		public void Add(K key)
		{
			AssertOpen();
			AssertHasFactory();

			var obj = Make(key);

			if (obj == null)
				return;

			if (!factory.Passivate(key, obj))
				return;

			if (!idle.ContainsKey(key))
				idle.Add(key, new Queue<V>());

			var idleQueue = idle[key];

			lock ((idleQueue as ICollection).SyncRoot)
				idleQueue.Enqueue(obj);
		}

		public void Clear()
		{
			var keys = idle.Keys.ToList();
			foreach (var key in keys)
				Clear(key);
		}

		public void Clear(K key)
		{
			if (!idle.ContainsKey(key))
				return;

			var idleQueue = idle[key];

			var items = new List<V>();

			lock ((idleQueue as ICollection).SyncRoot)
			{
				items.AddRange(idleQueue);
				idleQueue.Clear();
				idle.Remove(key);
			}

			if (items.Count == 0)
				return;

			AssertHasFactory();
		
			foreach (var obj in items)
				Destroy(key, obj);
		}

		public void Close()
		{
			if (IsClosed)
				return;

			IsClosed = true;
			Clear();
		}

		public void SetFactory(IKeyedPoolableObjectFactory<K, V> factory)
		{
			var oldFactory = this.factory;
			var idleObjs = new Dictionary<K, List<V>>();

			lock (this)
			{
				AssertOpen();
				if (GetActiveCount() > 0)
					throw new InvalidOperationException("Cannot change factory with active object in the pool.");
				else
				{
					foreach (var key in idle.Keys)
					{
						idleObjs.Add(key, new List<V>());
						idleObjs[key].AddRange(idle[key]);
						idle[key].Clear();
					}
					idle.Clear();
				}
				this.factory = factory;
			}

			foreach (var entry in idleObjs)
				foreach(var obj in entry.Value)
					oldFactory.Destroy(entry.Key, obj);
		}

		#endregion

		#region Counter Helpers

		long AvailableDecrement(K key)
		{
			if (!available.ContainsKey(key))
				available.Add(key, new Counter(MaxSize));
			return available[key].Decrement();
		}

		long AvailableIncrement(K key)
		{
			if (!available.ContainsKey(key))
				available.Add(key, new Counter(MaxSize));
			return available[key].Increment();
		}

		long Decrement(K key, IDictionary<K, Counter> counters)
		{
			if (!counters.ContainsKey(key))
				counters.Add(key, new Counter());
			return counters[key].Decrement();
		}

		long Increment(K key, IDictionary<K, Counter> counters)
		{
			if (!counters.ContainsKey(key))
				counters.Add(key, new Counter());
			return counters[key].Increment();
		}

		#endregion

		V Acquire(K key)
		{
			if (HasMaxSize)
			{
//				int count = Interlocked.Decrement(ref available);
				long count = AvailableDecrement(key);
				if (count < 0)
				{
					AvailableIncrement(key);
					return null;
				}
			}

			try
			{
				var obj = FindIdleOrMake(key);
				return obj;
			}
			catch (Exception)
			{
				if (HasMaxSize)
					AvailableDecrement(key);
				throw;
			}
		}

		V FindIdleOrMake(K key)
		{
			V obj = null;

			if (!idle.ContainsKey(key))
				idle.Add(key, new Queue<V>());

			var idleQueue = idle[key];
			
			lock ((idleQueue as ICollection).SyncRoot)
			{
				while (idleQueue.Count > 0)
				{
					obj = idleQueue.Dequeue();

					try
					{
						factory.Activate(key, obj);
					}
					catch
					{
						Increment(key, failedActivateIdleCounts);
						continue;
					}

					if (!factory.Validate(key, obj))
					{
						Increment(key, failedValidateIdleCounts);
						continue;
					}

					Increment(key, reuseCounts);
					break;
				}
			}

			if (obj == null)
			{
				AssertHasFactory();

				obj = Make(key);

				if (obj == null)
					return null;

				try
				{
					factory.Activate(key, obj);
				}
				catch
				{
					Increment(key, failedActivateNewCounts);
					return null;
				}

				if (!factory.Validate(key, obj))
				{
					Increment(key, failedValidateNewCounts);
					return null;
				}
			}

			if (!active.ContainsKey(key))
				active.Add(key, new List<V>());

			var activeList = active[key];

			lock ((activeList as ICollection).SyncRoot)
			{
				activeList.Add(obj);
			}

			return obj;
		}

		V Make(K key)
		{
			AssertOpen();
			AssertHasFactory();

			try
			{
				var obj = factory.Make(key);
				Increment(key, makeCounts);
				return obj;
			}
			catch
			{
				Increment(key, failedMakeCounts);
			}
			return null;
		}

		void Destroy(K key, V obj)
		{
			if (key == null || obj == null)
				return;

			try
			{
				factory.Destroy(key, obj);
			}
			catch
			{
				Increment(key, failedDestroyCounts);
			}
		}

		void AssertOpen()
		{
			if (IsClosed)
				throw new InvalidOperationException("Pool is closed.");
		}

		void AssertHasFactory()
		{
			if (factory == null)
				throw new NullReferenceException("Pool is missing object factory.");
		}
	}
}
