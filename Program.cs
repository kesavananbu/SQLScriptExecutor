using System.Transactions;
using Microsoft.Data.SqlClient;
using System.Reflection;
using Microsoft.Extensions.Configuration;
public class Program
{
    static string _outputFilePath = "";
    public static void Main(string[] args)
    {
       var _configuration = new ConfigurationBuilder().AddJsonFile("AppSettings.json", optional: false, reloadOnChange: true).Build();

        var connectionString = _configuration.GetConnectionString("SqlServerConnection");
        var appSettings = _configuration.GetSection("AppSettings");

        var logFilePath = appSettings["LogFilePath"];
        var releaseScriptPath = appSettings["ReleaseScriptPath"];
        var timeoutInMinutes = int.Parse(appSettings["TimeoutInMinutes"]);
        var IgnoreFileLists = appSettings["IgnoreFileLists"];
        var IgnoreFolderLists = appSettings["IgnoreFolderLists"];
        var SingleTransactionScope = bool.Parse(appSettings["SingleTransactionScope"]);

        _outputFilePath = logFilePath;
        
        List<string> fileList = new List<string>();
        var ignoreFileLists = new HashSet<string>(IgnoreFileLists.Split(','));
        var ignoreFolderLists = new HashSet<string>(IgnoreFolderLists.Split(','));

        TransactionScope scope = null;
        try
        {
            string folderPath = Path.GetDirectoryName(_outputFilePath);
            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
                Console.WriteLine($"Folder '{folderPath}' created.");
            }
            if (File.Exists(_outputFilePath))
            {
                File.Delete(_outputFilePath);
            }

            if (!Directory.Exists(releaseScriptPath))
            {
                throw new($"ReleaseScript Folder {releaseScriptPath} doesn't exist !!");
            }

            GetFilesRecursively(releaseScriptPath, fileList, ignoreFileLists, ignoreFolderLists);

            // Set transaction timeout
            TimeSpan timeout = TimeSpan.FromMinutes(timeoutInMinutes);
            ConfigureTransactionTimeoutCore(timeout);

            connectionString.TrimEnd(';');
            connectionString += @$";Connection Timeout={(int)timeout.TotalSeconds}";
            if (!connectionString.Contains("encrypt=false", StringComparison.OrdinalIgnoreCase) && !connectionString.Contains("encrypt=true", StringComparison.OrdinalIgnoreCase))
            {
                connectionString += ";encrypt=false";
            }

            if (SingleTransactionScope && scope == null)
            {
                TransactionOptions transactionOptions = new TransactionOptions
                {
                    IsolationLevel = IsolationLevel.ReadCommitted,
                    Timeout = timeout
                };
                // Create a new transaction scope
                scope = new TransactionScope(TransactionScopeOption.RequiresNew, transactionOptions);
            }

            foreach (string file in fileList)
            {
                if(!SingleTransactionScope && scope == null)
                {
                    TransactionOptions transactionOptions = new TransactionOptions
                    {
                        IsolationLevel = IsolationLevel.ReadCommitted,
                        Timeout = timeout
                    };
                    scope = new TransactionScope(TransactionScopeOption.RequiresNew, transactionOptions);
                }

                AddLog($"Executing script {file} at {DateTime.Now}");

                
                string scriptContent = File.ReadAllText(file);

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    ExecuteScriptWithSSMS(connection, scriptContent, timeout);
                }

                if (!SingleTransactionScope && scope != null)
                {
                    scope.Complete();
                    scope.Dispose();
                    scope = null;
                }
                AddLog($"Execution completed for the script {file} at {DateTime.Now}");
            }

            AddLog( $"Completed the transaction at {DateTime.Now}");

            if(SingleTransactionScope && scope != null)
            {
                scope.Complete();
                scope.Dispose();
                scope = null;
            }
        }
        catch (Exception ex)
        {
            AddLog( $"Error Message: {ex.Message}");
        }
        finally
        {
            scope?.Dispose();
        }
    }

    public static void SetMaxTransactionTimeout(TimeSpan value)
    {
        var assembly = Assembly.GetAssembly(typeof(TransactionManager));
        var instance = assembly?.CreateInstance("System.Transactions.Configuration.MachineSettingsSection");
        var instanceType = instance?.GetType();
        var sMaxTimeout = instanceType?.GetFields(BindingFlags.Static | BindingFlags.NonPublic)
            .SingleOrDefault(f => f.Name.Contains("maxTimeout", StringComparison.OrdinalIgnoreCase));
        sMaxTimeout?.SetValue(null, value);
    }

    public static void ConfigureTransactionTimeoutCore(TimeSpan timeout)
    {
        SetTransactionManagerField("s_cachedMaxTimeout", true);
        SetTransactionManagerField("s_maximumTimeout", timeout);
    }

    static private void SetTransactionManagerField(string fieldName, object value)
    {
        var cacheField = typeof(TransactionManager).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static);
        cacheField.SetValue(null, value);
    }

    public static void GetFilesRecursively(string folderPath, List<string> fileList, HashSet<string> IgnoreFileLists, HashSet<string> IgnoreFolderLists)
    {
        foreach (string subfolder in Directory.GetDirectories(folderPath))
        {
            if (!IgnoreFolderLists.Contains(Path.GetFileName(subfolder), StringComparer.OrdinalIgnoreCase))
            {
                GetFilesRecursively(subfolder, fileList, IgnoreFileLists, IgnoreFolderLists);
            }
            else
            {
                AddLog(@$"Folder Ignored {subfolder}");
            }
        }

        foreach (string file in Directory.GetFiles(folderPath))
        {
            if(!IgnoreFileLists.Contains(Path.GetFileName(file), StringComparer.OrdinalIgnoreCase))
            {
                fileList.Add(file);
            }
            else
            {
                AddLog(@$"File Ignored {file}");
            }
        }
    }
    public static void ExecuteScriptWithSSMS(SqlConnection conn, string script, TimeSpan timeout)
    {
        var serverConnection = new Microsoft.SqlServer.Management.Common.ServerConnection(conn);
        serverConnection.StatementTimeout = (int)timeout.TotalSeconds;
        var server = new Microsoft.SqlServer.Management.Smo.Server(serverConnection);
        server.ConnectionContext.InfoMessage += ConnectionContext_InfoMessage;
        server.ConnectionContext.ServerMessage += ConnectionContext_ServerMessage;
        server.ConnectionContext.ExecuteNonQuery(script);
    }

    public static void ConnectionContext_ServerMessage(object sender, Microsoft.SqlServer.Management.Common.ServerMessageEventArgs e)
    {
        AddLog("Method : ConnectionContext_ServerMessage" + System.Environment.NewLine + e.Error.Message + System.Environment.NewLine);
    }

    public static void ConnectionContext_InfoMessage(object sender, SqlInfoMessageEventArgs e)
    {
        AddLog(System.Environment.NewLine + e.Message + System.Environment.NewLine);
    }

    public static void AddLog(string message)
    {
        File.AppendAllText(_outputFilePath, "\n"+ message);
        Console.WriteLine(message);
    }
}
