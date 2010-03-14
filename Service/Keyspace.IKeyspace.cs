using System;
using System.Collections.Generic;
using Apache.Cassandra;

namespace HectorSharp.Service
{
	partial class Keyspace : IKeyspace
	{
		#region IKeyspace Members
		public string Name { get; private set; }
		public IDictionary<string, IDictionary<string, string>> Description { get; private set; }
		public ICassandraClient Client { get; private set; }
		public ConsistencyLevel ConsistencyLevel { get; private set; }
		public FailoverPolicy FailoverPolicy { get; private set; }

		public Column GetColumn(string key, ColumnPath columnPath)
		{
			valideColumnPath(columnPath);

			var op = new Operation<Column>(ClientCounter.READ_FAIL);
			op.Handler = client =>
			{
				try
				{
					var cosc = client.get(Name, key, columnPath, ConsistencyLevel);
					return cosc == null ? null : cosc.Column;
				}
				catch (NotFoundException ex)
				{
					op.Error = ex;
				}
				return null;
			};

			OperateWithFailover(op);

			if (op.HasError)
				throw op.Error;
			return op.Result;
		}

		public SuperColumn GetSuperColumn(string key, ColumnPath columnPath)
		{
			return GetSuperColumn(key, columnPath, false, Int32.MaxValue);
		}

		//Override
		public SuperColumn GetSuperColumn(string key, ColumnPath columnPath, bool reversed, int size)
		{
			valideSuperColumnPath(columnPath);

			var sliceRange = new SliceRange(new byte[0], new byte[0], reversed, size);

			var op = new Operation<SuperColumn>(ClientCounter.READ_FAIL,
				client =>
				{
					var columnParent = new ColumnParent(columnPath.Column_family, columnPath.Super_column);
					var predicate = new SlicePredicate(null, sliceRange);
					var data = client.get_slice(Name, key, columnParent, predicate, ConsistencyLevel);
					return new SuperColumn(columnPath.Super_column, GetColumnList(data));
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IEnumerable<Column> GetSlice(string key, ColumnParent columnParent, SlicePredicate predicate)
		{
			var op = new Operation<IEnumerable<Column>>(ClientCounter.READ_FAIL,
				client =>
				{
					return Transform(
						client.get_slice(Name, key, columnParent, predicate, ConsistencyLevel),
						c => c.Column);
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IEnumerable<SuperColumn> GetSuperSlice(string key, ColumnParent columnParent, SlicePredicate predicate)
		{
			var op = new Operation<IEnumerable<SuperColumn>>(ClientCounter.READ_FAIL,
				client =>
				{
					return Transform(
						client.get_slice(Name, key, columnParent, predicate, ConsistencyLevel),
						c => c.Super_column
					);
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IDictionary<string, Column> MultigetColumn(IList<string> keys, ColumnPath columnPath)
		{
			valideColumnPath(columnPath);

			var op = new Operation<IDictionary<string, Column>>(ClientCounter.READ_FAIL,
				client =>
				{
					var result = new Dictionary<string, Column>();
					var cfmap = client.multiget(Name, keys, columnPath, ConsistencyLevel);

					foreach (var entry in Transform(cfmap, entry => new { entry.Key, entry.Value.Column }))
						result.Add(entry.Key, entry.Column);

					return result;
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IDictionary<string, SuperColumn> MultigetSuperColumn(IList<string> keys, ColumnPath columnPath)
		{
			return MultigetSuperColumn(keys, columnPath, false, Int32.MaxValue);
		}

		public IDictionary<string, SuperColumn> MultigetSuperColumn(IList<string> keys, ColumnPath columnPath, bool reversed, int size)
		{
			valideSuperColumnPath(columnPath);

			var result = new Dictionary<string, SuperColumn>();

			// only can get supercolumn by multigetSuperSlice
			var clp = new ColumnParent(columnPath.Column_family, columnPath.Super_column);
			var sr = new SliceRange(new byte[0], new byte[0], reversed, size);
			var sp = new SlicePredicate(null, sr);
			var sclist = MultigetSuperSlice(keys, clp, sp);

			if (sclist == null || sclist.Count == 0)
				return result;

			foreach (var sc in sclist)
				if (sc.Value.Count > 0)
					result.Add(sc.Key, sc.Value[0]);

			return result;
		}

		public IDictionary<string, IList<Column>> MultigetSlice(IList<string> keys, ColumnParent columnParent, SlicePredicate predicate)
		{
			var op = new Operation<IDictionary<string, IList<Column>>>(ClientCounter.READ_FAIL,
				client =>
				{
					var result = new Dictionary<string, IList<Column>>();
					var cfmap = client.multiget_slice(Name, keys, columnParent, predicate, ConsistencyLevel);
					foreach (var entry in Transform(cfmap, m => new { m.Key, List = GetColumnList(m.Value) }))
						result.Add(entry.Key, entry.List);
					return result;

				});
			OperateWithFailover(op);
			return op.Result;
		}

		public IDictionary<string, IList<SuperColumn>> MultigetSuperSlice(IList<string> keys, ColumnParent columnParent, SlicePredicate predicate)
		{
			var op = new Operation<IDictionary<string, IList<SuperColumn>>>(ClientCounter.READ_FAIL,
				client =>
				{
					var result = new Dictionary<string, IList<SuperColumn>>();
					var cfmap = client.multiget_slice(Name, keys, columnParent, predicate, ConsistencyLevel);

					if (columnParent.Super_column == null)
					{
						foreach (var entry in Transform(cfmap, m => new { m.Key, List = GetSuperColumnList(m.Value) }))
							result.Add(entry.Key, entry.List);
					}
					else
					{
						foreach (var entry in Transform(cfmap, m =>
							new
							{
								m.Key,
								List = new List<SuperColumn> 
								{ 
									new SuperColumn(columnParent.Super_column, GetColumnList(m.Value)) 
								}
							}))
							result.Add(entry.Key, entry.List);
					}
					return result;
				}
			);
			OperateWithFailover(op);
			return op.Result;
		}

		public IDictionary<string, IList<Column>> GetRangeSlice(ColumnParent columnParent, SlicePredicate predicate, string start, string finish, int count)
		{
			var op = new Operation<IDictionary<string, IList<Column>>>(ClientCounter.READ_FAIL,
				client =>
				{
					var result = new Dictionary<string, IList<Column>>();
					var keySlices = client.get_range_slice(Name, columnParent, predicate, start, finish, count, ConsistencyLevel);
					if (keySlices == null || keySlices.Count == 0)
						return result;

					foreach (var entry in Transform(keySlices, entry => new { entry.Key, Columns = GetColumnList(entry.Columns) }))
						result.Add(entry.Key, entry.Columns);

					return result;
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IDictionary<string, IList<SuperColumn>> GetSuperRangeSlice(ColumnParent columnParent, SlicePredicate predicate, string start, string finish, int count)
		{
			var op = new Operation<IDictionary<string, IList<SuperColumn>>>(ClientCounter.READ_FAIL,
				client =>
				{
					var result = new Dictionary<string, IList<SuperColumn>>();
					var keySlices = client.get_range_slice(Name, columnParent, predicate, start, finish, count, ConsistencyLevel);
					if (keySlices == null || keySlices.Count == 0)
						return result;

					foreach (var entry in Transform(keySlices, entry => new { entry.Key, Columns = GetSuperColumnList(entry.Columns) }))
						result.Add(entry.Key, entry.Columns);

					return result;
				}
			);
			OperateWithFailover(op);
			return op.Result;
		}

		public void Insert(string key, ColumnPath columnPath, byte[] value)
		{
			valideColumnPath(columnPath);

			var op = new VoidOperation(ClientCounter.WRITE_FAIL,
				client =>
				{
					client.insert(Name, key, columnPath, value, createTimeStamp(), ConsistencyLevel);
				});
			OperateWithFailover(op);
		}

		public void BatchInsert(string key, IDictionary<string, IList<Column>> columnMap, IDictionary<string, IList<SuperColumn>> superColumnMap)
		{
			if (columnMap == null && superColumnMap == null)
				throw new Exception("columnMap and SuperColumnMap can not be null at same time");

			var cfmap = new Dictionary<string, List<ColumnOrSuperColumn>>();

			foreach (var map in columnMap)
				cfmap.Add(map.Key, GetSoscList(map.Value));

			foreach (var map in superColumnMap)
				cfmap.Add(map.Key, GetSoscSuperList(map.Value));

			var op = new VoidOperation(ClientCounter.WRITE_FAIL,
				client => client.batch_insert(Name, key, cfmap, ConsistencyLevel)
			);

			OperateWithFailover(op);
		}

		public void Remove(string key, ColumnPath columnPath)
		{
			var op = new VoidOperation(ClientCounter.WRITE_FAIL,
				client =>
				{
					client.remove(Name, key, columnPath, createTimeStamp(), ConsistencyLevel);
				}
			);
			OperateWithFailover(op);
		}

		public int GetCount(string key, ColumnParent columnParent)
		{
			var op = new Operation<int>(ClientCounter.READ_FAIL,
				client =>
				{
					return client.get_count(Name, key, columnParent, ConsistencyLevel);
				});

			OperateWithFailover(op);
			return op.Result;
		}

		#endregion
	}
}
