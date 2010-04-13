HectorSharp Unit tests

Dependencies: Moq, XUnit, HectorSharp, Thrift

The Xunit testing framework is employed to attribute and run these tests.

Tests are broken down into 2 folders: 0.5.1 and 0.6.0.  Any future tests targetting 
specific versions of Cassandra will be organized into the appropriate folders.  We
will try to maintain tests for the last major version, to facilitate data migration
from previous versions.

There is currently no embedded testing server for cassandra.  Until one is produced,
tests will need to be integrated with a locally hosted server with thrift port on 9160.

Theres a RunCassandra.bat and CleanCassandraData.bat in the 0.6.0 directory that assumes
that the cassandra home should be c:\cassandra060, creates a drive substitution for
V:\ to the 0.6.0 directory and sets the %CASSANDRA_CONF% variable to V:\conf.  The data
directories are created at V:\var, which is the path that is deleted when running the
CleanCassandraData.bat.

Where we need to test load balancing or failover, we will mock the interfaces to 
simulate the behavior.  We may change other tests to use mocks as well, but for now
we're using integration tests, to help verify protocol interrop.

TODO:

. Add code to automatically run the RunCassandra.bat before the test run, and
	CleanCassandraData.bat after.
. Write tests for some known issues.  Looking for more feedback and bug reports.
