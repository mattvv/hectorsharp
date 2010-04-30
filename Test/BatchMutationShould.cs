using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace HectorSharp.Test
{
	public class BatchMutationShould 
	{
		IList<string> cfs = new List<string> { "Standard1" };

		[Fact]
		public void AddColumn()
		{
			var bm = new BatchMutation();
			var column = new Column("c_name", "c_val");
			bm.AddColumn("key1", cfs, column);

			Assert.Equal(1, bm["key1"].Count);

			var column2 = new Column("c_name2", "c_val2");
			bm.AddColumn("key1", cfs, column2);
			Assert.Equal(2, bm["key1"]["Standard1"].Count);
		}

		[Fact]
		public void AddSuperColumn()
		{
			var bm = new BatchMutation();
			var sc = new SuperColumn("sc_name", new List<Column>
			{
				new Column("c_name1", "c_val1"),
				new Column("c_name2", "c_val2"),
			});
			bm.AddSuperColumn("key1", cfs, sc);
			Assert.Equal(1, bm["key1"].Count);
			var sc2 = new SuperColumn("sc_name2", new List<Column>
			{
				new Column("c_name1", "c_val1")
			});
			bm.AddSuperColumn("key1", cfs, sc2);
			Assert.Equal(2, bm["key1"]["Standard1"].Count);
		}

		[Fact]
		public void AddDeletion()
		{
			var bm = new BatchMutation();
			var d1 = new Deletion();
			d1.SlicePredicate = new SlicePredicate();
			d1.SlicePredicate.ColumnNames.Add("c_name");
			bm.AddDeletion("key1", cfs, d1);
			Assert.Equal(1, bm["key1"].Count);

			var d2 = new Deletion();
			d2.SlicePredicate = new SlicePredicate();
			d2.SlicePredicate.ColumnNames.Add("c_name2");
			bm.AddDeletion("key1", cfs, d2);
			Assert.Equal(2, bm["key1"]["Standard1"].Count);
		}
	}
}
