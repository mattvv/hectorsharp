@echo off
@setlocal
@if not exist V:\ subst V: .
@V:
@cd \
@if exist var rmdir var /S /Q
@if exist V:\ subst V: /D