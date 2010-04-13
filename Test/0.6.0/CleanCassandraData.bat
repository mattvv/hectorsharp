@echo off
@setlocal
@if not exist V:\ subst V: .
@rmdir V:\var /S /Q
@if exist V:\ subst V: /D