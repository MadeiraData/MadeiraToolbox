<#
Update RegisterAllProvidersIP and HostRecordTTL in all AG Clusters
==================================================================
Author: Eitan Blumin | https://madeiradata.com
Date: 2021-06-21
Description:
This script is based on the recommendations on this page:
https://docs.microsoft.com/en-us/sql/database-engine/availability-groups/windows/create-or-configure-an-availability-group-listener-sql-server#FollowUp

This should be used for multi-subnet environments where it's NOT possible
for the applications to set "MultiSubnetFailover=True" in their connection strings.

This script is especially useful for when you have multiple AG clusters because it
automatically detects all failover clusters on the current machine.

If RegisterAllProvidersIP and/or HostRecordTTL should be changed, the relevant commands
will be printed out but will NOT actually be executed.

When ready, copy and paste the result commands and run them to actually apply the changes.

IMPORTANT NOTE: The output commands include restarting the cluster resources.
#>
Import-Module FailoverClusters  

cls
Get-ClusterResource | Where ResourceType -eq "Network Name" | ForEach {
    $Changed = $false
    $RegisterAllProvidersIP = $_  | Get-ClusterParameter -Name "RegisterAllProvidersIP"

    if ($RegisterAllProvidersIP.Value -eq 1) {
        "Get-ClusterResource '$($_.Name)' | Set-ClusterParameter RegisterAllProvidersIP 0"
        $Changed = $true
    }
    
    $HostRecordTTL = $_  | Get-ClusterParameter -Name "HostRecordTTL"

    if ($HostRecordTTL.Value -gt 300) {
        "Get-ClusterResource '$($_.Name)' | Set-ClusterParameter HostRecordTTL 300"
        $Changed = $true
    }

    if ($Changed) {
    "Stop-ClusterResource '$($_.Name)'"
    "Start-ClusterResource '$($_.Name)'"
    "Start-Clustergroup '$($_.OwnerGroup)'"
    }
}