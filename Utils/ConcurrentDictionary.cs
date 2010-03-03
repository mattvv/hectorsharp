using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Utils
{
    public class ConcurrentDictionary<TKey, TValue> : Dictionary<TKey, TValue>, IDictionary<TKey, TValue>
    {
        readonly object padlock = new object();

		  #region IDictionary<TKey,TValue> Members

		  void IDictionary<TKey, TValue>.Add(TKey key, TValue value)
		  {
			  lock (padlock)
				  this.Add(key, value);
		  }

		  bool IDictionary<TKey, TValue>.ContainsKey(TKey key)
		  {
			  return base.ContainsKey(key);
		  }

		  ICollection<TKey> IDictionary<TKey, TValue>.Keys
		  {
			  get { return base.Keys; }
		  }

		  bool IDictionary<TKey, TValue>.Remove(TKey key)
		  {
			  lock (padlock)
				  return base.Remove(key);
		  }

		  bool IDictionary<TKey, TValue>.TryGetValue(TKey key, out TValue value)
		  {
			  return base.TryGetValue(key, out value);
		  }

		  ICollection<TValue> IDictionary<TKey, TValue>.Values
		  {
			  get { return base.Values; }
		  }

		  TValue IDictionary<TKey, TValue>.this[TKey key]
		  {
			  get
			  {
				  return base[key];
			  }
			  set
			  {
				  base[key] = value;
			  }
		  }

		  #endregion

		  #region IEnumerable<KeyValuePair<TKey,TValue>> Members

		  IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
		  {
			  return base.GetEnumerator();
		  }

		  #endregion

		  #region IEnumerable Members

		  System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		  {
			  return base.GetEnumerator();
		  }

		  #endregion
	 }
}