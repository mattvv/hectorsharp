using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Utils.ObjectPool
{
	public interface IObjectPool<T> where T : class
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

	public interface IPoolableObjectFactory<T> where T : class
	{
		T Make();
		bool Destroy(T obj);
		bool Activate(T obj);
		bool Passivate(T obj);
		bool Validate(T obj);
	}
}
