using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Service
{
    public class PoolExhaustedException : Exception
    {

        private static sealed long serialVersionUID = -6200999597951673383L;

        public PoolExhaustedException(String msg) : base(msg)
        {
        }
    }
}
