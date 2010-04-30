using System;
using System.Collections.Generic;
using HectorSharp.Utils;

namespace HectorSharp
{
	internal partial class Keyspace : IKeyspace
	{
		#region IKeyspace Members
		public string Name { get; private set; }
		public IDictionary<string, Dictionary<string, string>> Description { get; private set; }
		public ICassandraClient Client { get; private set; }
		public ConsistencyLevel ConsistencyLevel { get; private set; }
		public FailoverPolicy FailoverPolicy { get; private set; }

		public Column GetColumn(string key, ColumnPath columnPath)
		{
			AssertColumnPath(columnPath);

			var op = new Operation<Column>(ClientCounter.READ_FAIL);
			op.Handler = client =>
			{
				try
				{
					var cosc = client.get(Name, key, columnPath.ToThrift(), ConsistencyLevel.ToThrift());
					return cosc == null ? null : cosc.Column.ToModel();
				}
				catch (Apache.Cassandra.NotFoundException ex)
				{
					op.Error = new NotFoundException("Column Not Found: key: " + key + ", " + columnPath.ToString(), ex);
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
			AssertSuperColumnPath(columnPath);

			var sliceRange = new Apache.Cassandra.SliceRange(reversed, size);

			var op = new Operation<SuperColumn>(ClientCounter.READ_FAIL,
				client =>
				{
					var columnParent = new Apache.Cassandra.ColumnParent(columnPath.ColumnFamily, columnPath.SuperColumn);
					var predicate = new Apache.Cassandra.SlicePredicate(null, sliceRange);
					var data = client.get_slice(Name, key, columnParent, predicate, ConsistencyLevel.ToThrift());
					return new SuperColumn(columnPath.SuperColumn, GetColumnList(data));
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IEnumerable<Column> GetSlice(string key, ColumnParent columnParent, SlicePredicate predicate)
		{
			var op = new Operation<IEnumerable<Column>>(ClientCounter.READ_FAIL,
				client =>
				{
					return client.get_slice(Name, key, columnParent.ToThrift(), predicate.ToThrift(), ConsistencyLevel.ToThrift())
						.Transform(c => c.Column.ToModel());
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IEnumerable<SuperColumn> GetSuperSlice(string key, ColumnParent columnParent, SlicePredicate predicate)
		{
			var op = new Operation<IEnumerable<SuperColumn>>(ClientCounter.READ_FAIL,
				client =>
				{
					return client.get_slice(Name, key, columnParent.ToThrift(), predicate.ToThrift(), ConsistencyLevel.ToThrift())
						.Transform(c => c.Super_column.ToModel());
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IDictionary<string, Column> MultigetColumn(IList<string> keys, ColumnPath columnPath)
		{
			AssertColumnPath(columnPath);

			var op = new Operation<IDictionary<string, Column>>(ClientCounter.READ_FAIL,
				client =>
				{
					var result = new Dictionary<string, Column>();

					var cfmap = client.multiget(Name, new List<string>(keys), columnPath.ToThrift(), ConsistencyLevel.ToThrift());

					foreach (var entry in cfmap.Transform(entry => new { entry.Key, Column = entry.Value.Column.ToModel() }))
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
			AssertSuperColumnPath(columnPath);

			var result = new Dictionary<string, SuperColumn>();

			// only can get supercolumn by multigetSuperSlice
			var clp = new ColumnParent(columnPath.ColumnFamily, columnPath.SuperColumn);
			var sr = new SliceRange(reversed, size);
			var sp = new SlicePredicate(sr);
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
					var cfmap = client.multiget_slice(Name, new List<string>(keys), columnParent.ToThrift(), predicate.ToThrift(), ConsistencyLevel.ToThrift());
					foreach (var entry in cfmap.Transform(m => new { m.Key, List = GetColumnList(m.Value) }))
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
					var cfmap = client.multiget_slice(Name, new List<string>(keys), columnParent.ToThrift(), predicate.ToThrift(), ConsistencyLevel.ToThrift());

					if (string.IsNullOrEmpty(columnParent.SuperColumn))
					{
						foreach (var entry in cfmap.Transform(m => new { m.Key, List = GetSuperColumnList(m.Value) }))
							result.Add(entry.Key, entry.List);
					}
					else
					{
						foreach (var entry in cfmap.Transform(m =>
							new
							{
								m.Key,
								List = new List<SuperColumn> 
								{ 
									new SuperColumn(columnParent.SuperColumn, GetColumnList(m.Value)) 
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
					var keyRange = new Apache.Cassandra.KeyRange();
					keyRange.Start_key = start;
					keyRange.End_key = finish;
					keyRange.Count = count;

					// deprecated
					//	var keySlices = client.get_range_slices(Name, columnParent.ToThrift(), predicate.ToThrift(), start, finish, count, ConsistencyLevel.ToThrift());

					var keySlices = client.get_range_slices(Name, columnParent.ToThrift(), predicate.ToThrift(), keyRange, ConsistencyLevel.ToThrift());
					if (keySlices == null || keySlices.Count == 0)
						return result;

					foreach (var entry in keySlices.Transform(entry => new { entry.Key, Columns = GetColumnList(entry.Columns) }))
						result.Add(entry.Key, entry.Columns);

					return result;
				});

			OperateWithFailover(op);
			return op.Result;
		}

		public IDictionary<string, IList<Column>> GetRangeSlices(ColumnParent columnParent, SlicePredicate predicate, KeyRange keyRange)
		{
			var op = new Operation<IDictionary<string, IList<Column>>>(ClientCounter.READ_FAIL,
				client =>
				{
					var result = new Dictionary<string, IList<Column>>();

					var keySlices = client.get_range_slices(Name, columnParent.ToThrift(), predicate.ToThrift(), keyRange.ToThrift(), ConsistencyLevel.ToThrift());
					if (keySlices == null || keySlices.Count == 0)
						return result;

					foreach (var entry in keySlices.Transform(entry => new { entry.Key, Columns = GetColumnList(entry.Columns) }))
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
					var keySlices = client.get_range_slice(Name, columnParent.ToThrift(), predicate.ToThrift(), start, finish, count, ConsistencyLevel.ToThrift());
					if (keySlices == null || keySlices.Count == 0)
						return result;

					foreach (var entry in keySlices.Transform(entry => new { entry.Key, Columns = GetSuperColumnList(entry.Columns) }))
						result.Add(entry.Key, entry.Columns);

					return result;
				}
			);
			OperateWithFailover(op);
			return op.Result;
		}

		public void Insert(string key, ColumnPath columnPath, byte[] value)
		{
			AssertColumnPath(columnPath);

			var op = new VoidOperation(ClientCounter.WRITE_FAIL,
				client =>
				{
					client.insert(Name, key, columnPath.ToThrift(), value, Util.UnixTimestamp, ConsistencyLevel.ToThrift());
				});
			OperateWithFailover(op);
		}

		public void Insert(string key, ColumnPath columnPath, string value)
		{
			Insert(key, columnPath, value.UTF());
		}

		public void BatchInsert(string key, IDictionary<string, IList<Column>> columnMap, IDictionary<string, IList<SuperColumn>> superColumnMap)
		{
			if (columnMap == null && superColumnMap == null)
				throw new Exception("columnMap and SuperColumnMap can not be null at same time");

			var cfmap = new Dictionary<string, List<Apache.Cassandra.ColumnOrSuperColumn>>();

			if (columnMap != null)
				foreach (var map in columnMap)
					cfmap.Add(map.Key, GetSoscList(map.Value));

			if (superColumnMap != null)
				foreach (var map in superColumnMap)
					cfmap.Add(map.Key, GetSoscSuperList(map.Value));

			var op = new VoidOperation(ClientCounter.WRITE_FAIL,
				client => client.batch_insert(Name, key, cfmap, ConsistencyLevel.ToThrift())
			);

			OperateWithFailover(op);
		}

		public void BatchMutate(IDictionary<string, IDictionary<string, IList<Mutation>>> mutationMap)
		{
			throw new NotImplementedException();
		}

		public void BatchMutate(BatchMutation batchMutation)
		{
			throw new NotImplementedException();
		}

		public void Remove(string key, ColumnPath columnPath)
		{
			var op = new VoidOperation(ClientCounter.WRITE_FAIL,
				client =>
				{
					client.remove(Name, key, columnPath.ToThrift(), Util.UnixTimestamp, ConsistencyLevel.ToThrift());
				}
			);
			OperateWithFailover(op);
		}

		public int GetCount(string key, ColumnParent columnParent)
		{
			var op = new Operation<int>(ClientCounter.READ_FAIL,
				client =>
				{
					return client.get_count(Name, key, columnParent.ToThrift(), ConsistencyLevel.ToThrift());
				});

			OperateWithFailover(op);
			return op.Result;
		}


		#endregion
	}
}
