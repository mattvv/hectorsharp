using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Utils.ObjectPool
{
	// Object Pool interfaces 

	public interface IObjectPool<T>
		  where T : class
	{
		int Active { get; }
		int Idle { get; }

		T Borrow();
		void Return(T obj);
		void Add();
		void Clear();
		void Close();
		void SetFactory(IPoolableObjectFactory<T> factory);
	}

	public interface IKeyedObjectPool<K, V>
		where K : IComparable
		where V : class
	{
		IEnumerable<K> Keys { get; }
		bool HasMaxSize { get; }

		int GetActiveCount();
		int GetActiveCount(K key);
		int GetIdleCount();
		int GetIdleCount(K key);

		V Borrow(K key);
		void Return(K key, V obj);
		void Add(K key);
		void Clear();
		void Clear(K key);
		void Close();

		void SetFactory(IKeyedPoolableObjectFactory<K, V> factory);
	}

	// Object pool factory interfaces

	public interface IObjectPoolFactory<T>
		 where T : class
	{
		IObjectPool<T> Create();
	}

	public interface IKeyedObjectPoolFactory<K, V>
		where K : IComparable
		where V : class
	{
		IKeyedObjectPool<K, V> Create();
	}

	// Poolable object factory interfaces

	public interface IPoolableObjectFactory<T>
		  where T : class
	{
		T Make();
		void Destroy(T obj);
		void Activate(T obj);
		bool Passivate(T obj);
		bool Validate(T obj);
	}

	public interface IKeyedPoolableObjectFactory<K, V>
		where K : IComparable
		where V : class
	{
		V Make(K key);
		void Destroy(K key, V obj);
		void Activate(K key, V obj);
		bool Passivate(K key, V obj);
		bool Validate(K key, V obj);
	}
}