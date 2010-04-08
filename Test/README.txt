HectorSharp Unit tests

Dependencies: Moq, XUnit, HectorSharp, Thrift

The Xunit testing framework is employed to attribute and run these tests.

Tests are broken down into 2 folders: 0.5.1 and 0.6.0.  Any future tests targetting 
specific versions of Cassandra will be organized into the appropriate folders.  We
will try to maintain tests for the last major version, to facilitate data migration
from previous versions.

There is currently no embedded testing server for cassandra.  Until one is produced,
tests will need to be integrated with a locally hosted server with thrift port on 9160.

Where we need to test load balancing or failover, we will mock the interfaces to 
simulate the behavior.  We may change other tests to use mocks as well, but for now
we're using integration tests, to help verify protocol interrop.

TODO:

. Finish porting remaining Java (JUnit) tests.
. Port 0.5.1 unit tests to the 0.6.0 cassandra api.
. Write tests for some known issues.  Looking for more feedback and bug reports.
