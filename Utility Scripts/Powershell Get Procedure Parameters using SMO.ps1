param
(
    [string]$InstanceName = $env:COMPUTERNAME,
    [string]$DBName = "MyDB",
    [string]$ProcedureSchema = "dbo",
    [string]$ProcedureName = "SQLLogAdd"
)

[System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO') | Out-Null

$serverInstance = New-Object ('Microsoft.SqlServer.Management.Smo.Server') $InstanceName

$serverInstance.SetDefaultInitFields([Microsoft.SqlServer.Management.Smo.StoredProcedure], $false) # Limit SMO retrieval to procedures only

$procedure = $serverInstance.Databases[$DBName].StoredProcedures[$ProcedureName, $ProcedureSchema];

$procedure.Parameters | Select-Object Name, DataType, DefaultValue, @{Name="Properties";Expression={$_.Properties | Where Name -in "Length", "NumericPrecision", "NumericScale", "IsOutputParameter" | Select Name, Value }}


# $procedure.Parameters[0].Properties | Select Name, Value       # <--- run this to see a list of available parameter properties