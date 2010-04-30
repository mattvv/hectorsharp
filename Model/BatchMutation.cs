using System.Collections.Generic;

namespace HectorSharp
{
	/// <summary>
	/// Encapsulates a set of updates, insertions and deletions sent as a single
	/// call to Cassandra.
	/// </summary>
	public class BatchMutation : Dictionary<string, IDictionary<string, IList<Mutation>>>
	{
		public BatchMutation AddColumn(string key, IList<string> columnFamilies, Column column)
		{
			AddMutation(key, columnFamilies, new Mutation(new ColumnOrSuperColumn(column)));
			return this;
		}

		public BatchMutation AddSuperColumn(string key, IList<string> columnFamilies, SuperColumn superColumn)
		{
			AddMutation(key, columnFamilies, new Mutation(new ColumnOrSuperColumn(superColumn)));
			return this;
		}

		public BatchMutation AddDeletion(string key, IList<string> columnFamilies, Deletion deletion)
		{
			AddMutation(key, columnFamilies, new Mutation(deletion));
			return this;
		}

		void AddMutation(string key, IList<string> columnFamilies, Mutation mutation)
		{
			IDictionary<string, IList<Mutation>> map;

			if (ContainsKey(key))
				map = this[key];
			else
				map = new Dictionary<string, IList<Mutation>>();

			foreach (var cf in columnFamilies)
			{
				if (!map.ContainsKey(cf))
					map.Add(cf, new List<Mutation> { mutation });
				else
					map[cf].Add(mutation);
			}
			Add(key, map);
		}
	}
}