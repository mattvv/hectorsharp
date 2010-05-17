using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Moq;

namespace HectorSharp.Test
{
	public abstract class TestBase
	{
		// mock matchers
		protected T Any<T>(){ return It.IsAny<T>(); }
		protected string AnyString() { return Any<string>(); }
		protected bool AnyBool() { return Any<bool>(); }
		protected int AnyInt() { return Any<int>(); }
		protected long AnyLong() { return Any<long>(); }
		protected byte[] AnyBytes() { return Any<byte[]>(); }
		protected Endpoint IsEndpoint(Endpoint e) { return It.Is<Endpoint>(x => x.Host == e.Host); }
	}
}
