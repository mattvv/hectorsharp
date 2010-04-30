namespace HectorSharp
{
	public enum ConsistencyLevel
	{
		ZERO = 0,
		ONE = 1,
		QUORUM = 2,
		DCQUORUM = 3,
		DCQUORUMSYNC = 4,
		ALL = 5,
		ANY = 6,
	}

	public enum FailoverStrategy
	{
		// On communication failure, just return the error to the client and don't retry
		FAIL_FAST,
		// On communication error try one more server before giving up
		ON_FAIL_TRY_ONE_NEXT_AVAILABLE,
		// On communication error try all known servers before giving up 
		ON_FAIL_TRY_ALL_AVAILABLE
	}

	public enum LoadBalancingStrategy
	{
		RoundRobin, LeastUtilized
	}

	/// <summary>
	/// available client event counters
	/// </summary>
	public enum ClientCounter
	{
		RECOVERABLE_TIMED_OUT_EXCEPTIONS,
		RECOVERABLE_UNAVAILABLE_EXCEPTIONS,
		RECOVERABLE_TRANSPORT_EXCEPTIONS,
		SKIP_HOST_SUCCESS,
		WRITE_SUCCESS,
		WRITE_FAIL,
		READ_SUCCESS,
		READ_FAIL,
		POOL_EXHAUSTED,
		/** Load balance connection errors */
		RECOVERABLE_LB_CONNECT_ERRORS,
	}
}
