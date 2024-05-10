--Pack to single exe
dotnet publish -o PublishPacked -r win-x64 -c Release --self-contained true /p:PublishSingleFile=true
