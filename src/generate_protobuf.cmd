@echo off

svc\packages\Google.Protobuf.Tools.3.5.1\tools\windows_x64\protoc.exe --csharp_out=. --csharp_opt=file_extension=.pb.cs azbackup_proto.proto
IF %ERRORLEVEL% NEQ 0 pause
