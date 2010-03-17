using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace HectorSharp.Service
{
	/// <summary>
	/// The keyspace is a high level handle to all read/write operations to cassandra.
	/// A Keyspace object is not thread safe. Use one keyspace per thread please!
	/// </summary>
	public interface IKeyspace
	{
		string Name { get; }
		IDictionary<string, IDictionary<string, string>> Description { get; }
		ConsistencyLevel ConsistencyLevel { get; }
		FailoverPolicy FailoverPolicy { get; }
		ICassandraClient Client { get; }

		/// <summary>
		/// Get the Column at the given columnPath.
		/// If no value is present, NotFoundException is thrown.
		/// @throws NotFoundException
		///	if no value exists for the column
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnPath"></param>
		/// <returns></returns>
		Column GetColumn(string key, ColumnPath columnPath);

		/// <summary>
		/// Get the SuperColumn at the given columnPath.
		/// If no value is present, NotFoundException is thrown.
		/// by default will return column with native order and the size of the list is
		/// unlimited (so be careful...)
		/// 
		/// @throws NotFoundException
		///	when a supercolumn is not found
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnPath"></param>
		/// <returns></returns>
		SuperColumn GetSuperColumn(string key, ColumnPath columnPath);
		
		/// <summary>
		/// Get the SuperColumn at the given columnPath.
		/// If no value is present, NotFoundException is thrown.
		/// 		@throws NotFoundException
		/// 		when a supercolumn is not found
		/// </summary>
		/// <remarks>by default will return column with native order and the size of the list is unlimited (so be careful...)</remarks>
		/// <param name="key"></param>
		/// <param name="columnPath"></param>
		/// <param name="reversed">reverse column sort order</param>
		/// <param name="size">column size</param>
		/// <returns></returns>
		SuperColumn GetSuperColumn(string key, ColumnPath columnPath, bool reversed, int size);
		
		/// <summary>
		/// Get the group of columns contained by columnParent.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnParent"></param>
		/// <param name="predicate"></param>
		/// <returns>
		/// Either a ColumnFamily name or a ColumnFamily/SuperColumn specified by the given predicate. 
		/// If no matching values are found, an empty list
		/// </returns>
		IEnumerable<Column> GetSlice(string key, ColumnParent columnParent, SlicePredicate predicate);

		/// <summary>
		/// Get the group of superColumn contained by columnParent.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnParent"></param>
		/// <param name="predicate"></param>
		/// <returns></returns>
		IEnumerable<SuperColumn> GetSuperSlice(string key, ColumnParent columnParent, SlicePredicate predicate);

		/// <summary>
		/// Performs a get for columnPath in parallel on the given list of keys.
		/// </summary>
		/// <param name="keys"></param>
		/// <param name="columnPath"></param>
		/// <returns>
		/// The return value maps keys to the Column found. If no value corresponding
		/// to a key is present, the key will still be in the map, but both the column
		/// and superColumn references of the ColumnOrSuperColumn object it maps to
		/// will be null.
		/// </returns>
		IDictionary<string, Column> MultigetColumn(IList<string> keys, ColumnPath columnPath);

		/// <summary>
		/// Performs a get for columnPath in parallel on the given list of keys.
		/// </summary>
		/// <param name="keys"></param>
		/// <param name="columnPath"></param>
		/// <returns>
		/// The return value maps keys to the ColumnOrSuperColumn found. If no value
		/// corresponding to a key is present, the key will still be in the map, but
		/// both the column and superColumn references of the ColumnOrSuperColumn
		/// object it maps to will be null.
		/// </returns>
		IDictionary<string, SuperColumn> MultigetSuperColumn(IList<string> keys, ColumnPath columnPath);

		/// <summary>
		/// Perform a get for columnPath in parallel on the given list of keys.
		/// </summary>
		/// <param name="keys"></param>
		/// <param name="columnPath"></param>
		/// <param name="reversed"></param>
		/// <param name="size"></param>
		/// <returns>
		/// The return value maps keys to the ColumnOrSuperColumn found. If no value
		/// corresponding to a key is present, the key will still be in the map, but
		/// both the column and superColumn references of the ColumnOrSuperColumn
		/// object it maps to will be null.
		/// </returns>
		IDictionary<string, SuperColumn> MultigetSuperColumn(IList<string> keys, ColumnPath columnPath, bool reversed, int size);

		/// <summary>
		/// Performs a get_slice for columnParent and predicate for the given keys in parallel.
		/// </summary>
		/// <param name="keys"></param>
		/// <param name="columnParent"></param>
		/// <param name="predicate"></param>
		/// <returns></returns>
		IDictionary<string, IList<Column>> MultigetSlice(IList<string> keys, ColumnParent columnParent, SlicePredicate predicate);

		/// <summary>
		/// Performs a get_slice for a superColumn columnParent and predicate for the given keys in parallel.
		/// </summary>
		/// <param name="keys"></param>
		/// <param name="columnParent"></param>
		/// <param name="predicate"></param>
		/// <returns></returns>
		IDictionary<string, IList<SuperColumn>> MultigetSuperSlice(IList<string> keys, ColumnParent columnParent, SlicePredicate predicate);

		/// <summary>
		/// Inserts a column.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnPath"></param>
		/// <param name="value"></param>
		void Insert(string key, ColumnPath columnPath, byte[] value);
		void Insert(string key, ColumnPath columnPath, string value);

		/// <summary>
		/// Insert Columns or SuperColumns across different Column Families for the same row key.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="cfmap"></param>
		/// <param name="superColumnMap"></param>
		void BatchInsert(string key, IDictionary<string, IList<Column>> cfmap, IDictionary<string, IList<SuperColumn>> superColumnMap);

		/// <summary>
		/// Remove data from the row specified by key at the columnPath.
		/// </summary>
		/// <remarks>
		/// Note that all the values in columnPath besides columnPath.column_family are truly optional:
		/// you can remove the entire row by just specifying the ColumnFamily, or you can remove
		/// a SuperColumn or a single Column by specifying those levels too.
		/// </remarks>
		/// <param name="key"></param>
		/// <param name="columnPath"></param>
		void Remove(string key, ColumnPath columnPath);

		/// <summary>
		/// Counts the columns present in columnParent.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnParent"></param>
		/// <returns></returns>
		int GetCount(string key, ColumnParent columnParent);

		/// <summary>
		/// a subset of columns for a range of keys.
		/// </summary>
		/// <param name="columnParent"></param>
		/// <param name="predicate"></param>
		/// <param name="start"></param>
		/// <param name="finish"></param>
		/// <param name="count"></param>
		/// <returns></returns>
		IDictionary<string, IList<Column>> GetRangeSlice(
			ColumnParent columnParent, 
			SlicePredicate predicate,
			 string start, string finish, int count);

		/// <summary>
		/// a subset of super columns for a range of keys.
		/// </summary>
		/// <param name="columnParent"></param>
		/// <param name="predicate"></param>
		/// <param name="start"></param>
		/// <param name="finish"></param>
		/// <param name="count"></param>
		/// <returns></returns>
		IDictionary<string, IList<SuperColumn>> GetSuperRangeSlice(
			ColumnParent columnParent, 
			SlicePredicate predicate,
			string start, string finish, int count);
	}
}
