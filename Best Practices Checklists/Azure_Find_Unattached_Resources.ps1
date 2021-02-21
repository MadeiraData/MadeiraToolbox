param
(
[string]$Subscription = "Guy's MVP Extended Sandbox",
[int]$maxBlobCountToCheck = 10000
)

# install AZ modules
$modules = @("Az.Accounts", "Az.Compute", "Az.Storage", "Az.Network")
        
foreach ($module in $modules) {
    if (Get-Module -ListAvailable -Name $module) {
        Write-Verbose "$module already installed"
    } 
    else {
        Write-Information "Installing $module"
        Install-Module $module -Force -SkipPublisherCheck -Scope CurrentUser | Out-Null
        Import-Module $module -Force -PassThru -Scope Local | Out-Null
    }
}

# connect to Azure Subscription
Connect-Azaccount

Get-AzSubscription | Where-Object {$_.Name -eq $Subscription} | ForEach-Object {
    Write-Output "Switching to subscription '$($_.Name)' in TenantId '$($_.TenantId)'"
    $SubscriptionId = $_.Id
    Select-AzSubscription -TenantId $_.TenantId -SubscriptionId $SubscriptionId | Out-Null
}

Write-Output "finding unattached managed disks"
Get-AzDisk | Where-Object {$_.ManagedBy -eq $null} | Select Id

Write-Output "finding unattached NIC cards"
Get-AzNetworkInterface | Where-Object {$_.VirtualMachine -eq $null } | Select Id

Write-Output "finding unattached public-ips"
Get-AzPublicIpAddress | Where-Object {$_.IpConfiguration -eq $null } | Select Id

Write-Output "finding unattached unmanaged disks"
$storageAccounts = Get-AzStorageAccount
foreach($storageAccount in $storageAccounts){
     $storageKey = (Get-AzStorageAccountKey -ResourceGroupName $storageAccount.ResourceGroupName -Name $storageAccount.StorageAccountName)[0].Value
     $context = New-AzStorageContext -StorageAccountName $storageAccount.StorageAccountName -StorageAccountKey $storageKey
     $containers = Get-AzStorageContainer -Context $context
     foreach($container in $containers){
         #Fetch all the Page blobs with extension .vhd as only Page blobs can be attached as disk to Azure VMs
         Get-AzStorageBlob -Container $container.Name -Context $context -MaxCount $maxBlobCountToCheck | Where-Object {$_.BlobType -eq 'PageBlob' -and $_.Name.EndsWith('.vhd')} | Select ICloudBlob.Uri.AbsoluteUri, Id
     }
 }
