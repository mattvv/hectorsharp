@echo off
@setlocal
@set CASSANDRA_HOME=c:\cassandra060
@if not exist V:\ subst V: .
@set CASSANDRA_CONF=V:\conf
@cd %CASSANDRA_HOME%
@call bin\cassandra.bat -f