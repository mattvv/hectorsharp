using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace HectorSharp.Service
{
	/**
	 * The keyspace is a high level handle to all read/write operations to cassandra.
	 *
	 * A Keyspace object is not thread safe. Use one keyspace per thread please!
	 *
	 * @author rantav
	 */
	public interface IKeyspace
	{
		/**
		 * @return The cassandra client object used to obtain this KeySpace.
		 */
		ICassandraClient getClient();

		/**
		 * Get the Column at the given columnPath.
		 *
		 * If no value is present, NotFoundException is thrown.
		 *
		 * @throws NotFoundException
		 *           if no value exists for the column
		 */
		Column getColumn(String key, ColumnPath columnPath);

		/**
		 * Get the SuperColumn at the given columnPath.
		 *
		 * If no value is present, NotFoundException is thrown.
		 *
		 * by default will return column with native order and the size of the list is
		 * unlimited (so be careful...)
		 *
		 * @throws NotFoundException
		 *           when a supercolumn is not found
		 */
		SuperColumn getSuperColumn(String key, ColumnPath columnPath);

		/**
		 * Get the SuperColumn at the given columnPath.
		 *
		 * If no value is present, NotFoundException is thrown.
		 *
		 * by default will return column with native order and the size of the list is
		 * unlimited (so be careful...)
		 *
		 * @param reversed
		 *          the result Column sort
		 * @param size
		 *          the result column size
		 * @throws NotFoundException
		 *           when a supercolumn is not found
		 */
		SuperColumn getSuperColumn(String key, ColumnPath columnPath, boolean reversed, int size);

		/**
		 * Get the group of columns contained by columnParent.
		 *
		 * Returns Either a ColumnFamily name or a ColumnFamily/SuperColumn specified
		 * by the given predicate. If no matching values are found, an empty list is
		 * returned.
		 */
		List<Column> getSlice(String key, ColumnParent columnParent, SlicePredicate predicate);

		/**
		 * Get the group of superColumn contained by columnParent.
		 */
		List<SuperColumn> getSuperSlice(String key, ColumnParent columnParent, SlicePredicate predicate);

		/**
		 * Performs a get for columnPath in parallel on the given list of keys.
		 *
		 * The return value maps keys to the Column found. If no value corresponding
		 * to a key is present, the key will still be in the map, but both the column
		 * and superColumn references of the ColumnOrSuperColumn object it maps to
		 * will be null.
		 */
		Dictionary<String, Column> multigetColumn(List<String> keys, ColumnPath columnPath);

		/**
		 * Performs a get for columnPath in parallel on the given list of keys.
		 *
		 * The return value maps keys to the ColumnOrSuperColumn found. If no value
		 * corresponding to a key is present, the key will still be in the map, but
		 * both the column and superColumn references of the ColumnOrSuperColumn
		 * object it maps to will be null.
		 */
		Dictionary<String, SuperColumn> multigetSuperColumn(List<String> keys, ColumnPath columnPath);

		/**
		 * Perform a get for columnPath in parallel on the given list of keys.
		 *
		 * The return value maps keys to the ColumnOrSuperColumn found. If no value
		 * corresponding to a key is present, the key will still be in the map, but
		 * both the column and superColumn references of the ColumnOrSuperColumn
		 * object it maps to will be null.
		 */
		Dictionary<String, SuperColumn> multigetSuperColumn(List<String> keys, ColumnPath columnPath, bool reversed, int size);

		/**
		 * Performs a get_slice for columnParent and predicate for the given keys in
		 * parallel.
		 */
		Dictionary<String, List<Column>> multigetSlice(List<String> keys, ColumnParent columnParent, SlicePredicate predicate);

		/**
		 * Performs a get_slice for a superColumn columnParent and predicate for the
		 * given keys in parallel.
		 */
		Dictionary<String, List<SuperColumn>> multigetSuperSlice(List<String> keys, ColumnParent columnParent, SlicePredicate predicate);

		/**
		 * Inserts a column.
		 */
		void insert(String key, ColumnPath columnPath, byte[] value);

		/**
		 * Insert Columns or SuperColumns across different Column Families for the same row key.
		 */
		void batchInsert(String key, Map<String, List<Column>> cfmap, Dictionary<String, List<SuperColumn>> superColumnMap);

		/**
		 * Remove data from the row specified by key at the columnPath.
		 *
		 * Note that all the values in columnPath besides columnPath.column_family are truly optional:
		 * you can remove the entire row by just specifying the ColumnFamily, or you can remove
		 * a SuperColumn or a single Column by specifying those levels too.
		 */
		void remove(String key, ColumnPath columnPath);

		/**
		 * get a description of the specified keyspace
		 */
		Dictionary<String, Dictionary<String, String>> describeKeyspace();

		/**
		 * Counts the columns present in columnParent.
		 */
		int getCount(String key, ColumnParent columnParent);

		/**
		 * returns a subset of columns for a range of keys.
		 */
		Dictionary<String, List<Column>> getRangeSlice(ColumnParent columnParent, SlicePredicate predicate,
			 String start, String finish, int count);

		/**
		 * returns a subset of super columns for a range of keys.
		 */
		Dictionary<String, List<SuperColumn>> getSuperRangeSlice(ColumnParent columnParent, SlicePredicate predicate,
			 String start, String finish, int count);

		/**
		 * @return The consistency level held by this keyspace instance.
		 */
		ConsistencyLevel ConsistencyLevel { get; }

		String Name { get; }

		/**
		 * @return The failover policy used by this keyspace.
		 */
		FailoverPolicy FailoverPolicy { get; }
	}
}
