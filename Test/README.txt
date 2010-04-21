HectorSharp Unit tests

Dependencies: Moq, XUnit, HectorSharp, Thrift

The Xunit testing framework is employed to attribute and run these tests.

Running the tests rely on the CASSANDRA_HOME environment variable being set
to the path where Cassandra is unpacked.

Theres a RunCassandra.bat and CleanCassandraData.bat which creates a drive substitution
V:\ to the debug/release directory and sets the %CASSANDRA_CONF% variable to V:\conf.  
The data directories are created at V:\var, which is the path that is deleted when running the
CleanCassandraData.bat.

Where we need to test load balancing or failover, we will mock the interfaces to 
simulate the behavior.  We may change other tests to use mocks as well, but for now
we're using integration tests, to help verify protocol interrop.

TODO:

. Write tests for some known issues.  Looking for more feedback and bug reports.
