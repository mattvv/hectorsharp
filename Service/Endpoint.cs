using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Service
{
    public class Endpoint : IComparable<string>, IComparable
    {
        string key;

        public Endpoint(string host, int port)
        {
            if (string.IsNullOrEmpty(host)) throw new ArgumentNullException(host);
            if (port == 0) throw new ArgumentOutOfRangeException("port");

            Host = host;
            Port = port;
            key = String.Format("{0}:{1}", Host, Port);
        }

        public string Host { get; private set; }
        public int Port { get; private set; }

        public override string ToString()
        {
            return key;
        }

        #region IComparable<string> Members

        public int CompareTo(string other)
        {
            return string.Compare(key, other.ToString());
        }

        #endregion

        #region IComparable Members

        public int CompareTo(object obj)
        {
            return CompareTo(obj as string);
        }

        #endregion
    }
}
