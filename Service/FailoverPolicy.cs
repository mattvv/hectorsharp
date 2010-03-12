using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HectorSharp.Utils;

namespace HectorSharp.Service
{
	public class FailoverPolicy
	{
		/**
 * What should the client do if a call to cassandra node fails and we suspect that the node is
 * down. (e.g. it's a communication error, not an application error).
 *
 * {@value #FAIL_FAST} will return the error as is to the user and not try anything smart
 *
 * {@value #ON_FAIL_TRY_ONE_NEXT_AVAILABLE} will try one more random server before returning to the
 * user with an error
 *
 * {@value #ON_FAIL_TRY_ALL_AVAILABLE} will try all available servers in the cluster before giving
 * up and returning the communication error to the user.
 *
 */
		Counter retryCount;
		FailoverStrategy defaultStrategy = FailoverStrategy.FAIL_FAST;

		public FailoverPolicy(long retryCount)
		{
			this.retryCount = new Counter(retryCount);
			this.Strategy = defaultStrategy;
		}

		public long RetryCount { get { return retryCount.Value;} }
		public long IncrementRetryCount() { return retryCount.Increment(); }
		public FailoverStrategy Strategy { get; set; }
	}
}