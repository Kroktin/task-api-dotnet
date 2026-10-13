FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY TaskApi/TaskApi.csproj TaskApi/
RUN dotnet restore TaskApi/TaskApi.csproj
COPY TaskApi/ TaskApi/
RUN dotnet publish TaskApi/TaskApi.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "TaskApi.dll"]
