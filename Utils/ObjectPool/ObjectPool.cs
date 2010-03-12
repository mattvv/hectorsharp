using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections;

namespace HectorSharp.Utils.ObjectPool
{
	public class ObjectPool<T> : IObjectPool<T> where T : class
	{
		public class Configuration
		{
			public int MinSize { get; set; }
			public int MaxSize { get; set; }
			public int Timeout { get; set; }
		}

		IPoolableObjectFactory<T> factory;
		bool isClosed;
		int minSize;
		int maxSize;
		int timeout;
		int available;
		Queue<T> idlePool;
		List<T> activePool;
		AutoResetEvent autoResetEvent;

		public int MinSize { get { return minSize; } }
		public int MaxSize { get { return maxSize; } }
		public int Timeout { get { return timeout; } }
		public int Idle { get { return idlePool.Count; } }
		public int Active { get { return activePool.Count; } }
		public bool HasMaxSize { get { return maxSize > 0; } }
		public bool IsClosed { get { return isClosed; } }

		int borrowCount;
		int returnCount;
		int makeCount;
		int reuseCount;
		int failedMakeCount;
		int failedActivateIdleCount;
		int failedValidateIdleCount;
		int failedActivateNewCount;
		int failedValidateNewCount;
		int failedValidateReturnCount;
		int failedPassivateReturnCount;
		int failedDestroyCount;

		public int BorrowCount { get { return borrowCount; } }
		public int ReturnCount { get { return returnCount; } }
		public int MakeCount { get { return makeCount; } }
		public int ReuseCount { get { return reuseCount; } }
		public int FailedMakeCount { get { return failedMakeCount; } }
		public int FailedActivateIdleCount { get { return failedActivateIdleCount; } }
		public int FailedValidateIdleCount { get { return failedValidateIdleCount; } }
		public int FailedActivateNewCount { get { return failedActivateNewCount; } }
		public int FailedValidateNewCount { get { return failedValidateNewCount; } }
		public int FailedValidateReturnCount { get { return failedValidateReturnCount; } }
		public int FailedPassivateReturnCount { get { return failedPassivateReturnCount; } }
		public int FailedDestroyCount { get { return failedDestroyCount; } }

		public ObjectPool(IPoolableObjectFactory<T> factory, ObjectPool<T>.Configuration config)
		{
			this.isClosed = false;
			this.factory = factory;
			this.idlePool = new Queue<T>();
			this.activePool = new List<T>();
			this.autoResetEvent = new AutoResetEvent(false);
			this.minSize = config.MinSize;
			this.maxSize = config.MaxSize;
			this.timeout = config.Timeout;

			if (minSize > maxSize)
				minSize = maxSize;

			this.available = maxSize;

			for (var i = 0; i < minSize; i++)
				Add();

			autoResetEvent.Set();
		}

		public T Borrow()
		{
			AssertOpen();

			int fullTimeOut = (int)timeout * 1000;
			int timeOut = fullTimeOut;
			DateTime start = DateTime.Now;

			while (timeOut > 0)
			{
				var obj = Acquire();
				if (obj != null)
				{
					Interlocked.Increment(ref borrowCount);
					return obj;
				}

				if (!autoResetEvent.WaitOne(timeOut, false))
					break;

				if (IsClosed)
					return null;

				timeOut = fullTimeOut - (int)DateTime.Now.Subtract(start).TotalMilliseconds;
			}

			throw new TimeoutException(
				 String.Format(
					  "Exceeded the {0} second timeout while trying to borrow a new \"{1}\" from the pool.",
					  timeout,
					  typeof(T).Name));
		}

		public void Return(T obj)
		{
			lock ((activePool as ICollection).SyncRoot)
			{
				if (activePool.Contains(obj))
					activePool.Remove(obj);
			}

			AssertHasFactory();

			if (!factory.Validate(obj))
			{
				Interlocked.Increment(ref failedValidateReturnCount);

				Destroy(obj);
			}
			else
			{
				if (HasMaxSize && idlePool.Count >= maxSize)
				{
					Destroy(obj);
				}
				else
				{
					if (!factory.Passivate(obj))
					{
						Interlocked.Increment(ref failedPassivateReturnCount);
						return;
					}

					if (IsClosed)
					{
						Destroy(obj);
						return;
					}

					lock ((idlePool as ICollection).SyncRoot)
					{
						idlePool.Enqueue(obj);
					}
				}
			}

			if (HasMaxSize)
			{
				Interlocked.Increment(ref available);
			}
			autoResetEvent.Set();
			Interlocked.Increment(ref returnCount);
		}

		public void Add()
		{
			AssertOpen();
			AssertHasFactory();

			var obj = Make();

			if (obj == null)
				return;

			if (!factory.Passivate(obj))
				return;

			idlePool.Enqueue(obj);
		}

		public void Clear()
		{
			var items = new List<T>();

			lock ((idlePool as ICollection).SyncRoot)
			{
				items.AddRange(idlePool);
				idlePool.Clear();
			}

			if (items.Count == 0)
				return;

			AssertHasFactory();
			foreach (var obj in items)
			{
				Destroy(obj);
			}
		}

		public void Close()
		{
			if (IsClosed)
				return;

			isClosed = true;
			Clear();
		}

		public void SetFactory(IPoolableObjectFactory<T> factory)
		{
			var oldFactory = this.factory;
			var idleObjs = new List<T>();

			lock (this)
			{
				AssertOpen();
				if (Active > 0)
				{
					throw new InvalidOperationException("Cannot change factory with active object in the pool.");
				}
				else
				{
					idleObjs.AddRange(this.idlePool);
					idlePool.Clear();
				}
				this.factory = factory;
			}

			foreach (var obj in idleObjs)
				oldFactory.Destroy(obj);
		}

		private T Make()
		{
			AssertOpen();
			AssertHasFactory();

			try
			{
				var obj = factory.Make();
				Interlocked.Increment(ref makeCount);
				return obj;
			}
			catch
			{
				Interlocked.Increment(ref failedMakeCount);
			}
			return null;
		}

		private T FindIdleOrMake()
		{
			T obj = null;
			lock ((idlePool as ICollection).SyncRoot)
			{
				while (idlePool.Count > 0)
				{
					obj = idlePool.Dequeue();

					try
					{
						factory.Activate(obj);
					}
					catch
					{
						Interlocked.Increment(ref failedActivateIdleCount);
						continue;
					}

					if (!factory.Validate(obj))
					{
						Interlocked.Increment(ref failedValidateIdleCount);
						continue;
					}

					Interlocked.Increment(ref reuseCount);
					break;
				}
			}

			if (obj == null)
			{
				AssertHasFactory();

				obj = Make();

				if (obj == null)
					return null;

				try 
				{
					factory.Activate(obj);
				}
				catch
				{
					Interlocked.Increment(ref failedActivateNewCount);
					return null;
				}

				if (!factory.Validate(obj))
				{
					Interlocked.Increment(ref failedValidateNewCount);
					return null;
				}
			}

			lock ((activePool as ICollection).SyncRoot)
			{
				activePool.Add(obj);
			}

			return obj;
		}

		private T Acquire()
		{
			if (HasMaxSize)
			{
				int count = Interlocked.Decrement(ref available);
				if (count < 0)
				{
					Interlocked.Increment(ref available);
					return null;
				}
			}

			try
			{
				var obj = FindIdleOrMake();
				return obj;
			}
			catch (Exception)
			{
				if (HasMaxSize)
				{
					Interlocked.Increment(ref available);
				}
				throw;
			}
		}

		private void Destroy(T obj)
		{
			if (obj == null)
				return;

			try
			{
				factory.Destroy(obj);
			}
			catch
			{
				Interlocked.Increment(ref failedDestroyCount);
			}
		}

		private void AssertOpen()
		{
			if (IsClosed)
				throw new InvalidOperationException("Pool is not open.");
		}

		private void AssertHasFactory()
		{
			if (factory == null)
				throw new NullReferenceException("Pool is missing object factory.");
		}
	}

}
