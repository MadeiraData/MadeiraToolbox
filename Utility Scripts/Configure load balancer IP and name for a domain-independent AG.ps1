<#
.SYNOPSIS
Script to configure load balancer listener IP and name for a domain-independent Availability Group in a Windows Workgroup

Author: Eitan Blumin
Date: 2021-01-20

.DESCRIPTION
This script is adapted from the scripts provided in the following resources:

https://techcommunity.microsoft.com/t5/core-infrastructure-and-security/sql-server-workgroup-cluster-fcm-errors/ba-p/371387

https://docs.microsoft.com/en-us/azure/azure-sql/virtual-machines/windows/availability-group-load-balancer-portal-configure#configure-the-cluster-to-use-the-load-balancer-ip-address

.LINK
See Microsoft Docs issue #69173: https://github.com/MicrosoftDocs/azure-docs/issues/69173
#>
$ListenerName           = "<MyListenerName>"
$IPResourceName         = "<MyIPResourceName>" # the IP Address resource name
$ListenerILBIP          = "<n.n.n.n>" # the IP Address of the Internal Load Balancer (ILB). This is the static IP address for the load balancer you configured in the Azure portal.
[int]$ListenerProbePort = <nnnnn>
$ListenerSubnet         = "255.255.255.255"
$ClusterAGRole          = "<MyClusterRoleName>"
$ClusterNetworkName     = "<MyClusterNetworkName>" # the cluster network name (Use Get-ClusterNetwork on Windows Server 2012 of higher to find the name)

Import-Module FailoverClusters -Force | Out-Null

Add-ClusterResource -Name $IPResourceName -ResourceType "IP Address" -Group $ClusterAGRole
Get-ClusterResource -Name $IPResourceName | Set-ClusterParameter -Multiple @{"Address"="$ListenerILBIP";"ProbePort"=$ListenerProbePort;"SubnetMask"="$ListenerSubnet";"Network"="$ClusterNetworkName";"EnableDhcp"=0}

Add-ClusterResource -Name $ListenerName -Group $ClusterAGRole -ResourceType "Network Name"
Get-ClusterResource -Name $ListenerName | Set-ClusterParameter -Multiple @{"DnsName" = "$ListenerName";"RegisterAllProvidersIP" = 1} 
Set-ClusterResourceDependency -Resource $ListenerName -Dependency "[$IPResourceName]" 
Start-ClusterResource -Name $ListenerName -Verbose 

Stop-ClusterResource -Name $ClusterAGRole -Verbose 
Set-ClusterResourceDependency -Resource $ClusterAGRole -Dependency "[$ListenerName]" 

Start-ClusterResource -Name $ClusterAGRole -Verbose 
