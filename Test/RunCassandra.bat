@echo off
setlocal

rem set CASSANDRA_HOME=c:\cassandra060
if exist V:\ subst V: /D
subst V: .
set CASSANDRA_CONF=V:\conf
cd %CASSANDRA_HOME%
call bin\cassandra.bat 

endlocal