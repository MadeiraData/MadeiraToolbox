param
(
[string]$Subscription = "Your subscription name here",
[int]$maxBlobCountToCheck = 10000
)
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Bypass

# install AZ modules
 Find-Module -Name Az -Repository PSGallery | Install-Module -Verbose -Force -Scope CurrentUser | Out-Null
 Set-ExecutionPolicy Unrestricted
 Import-Module -Name Az -Scope Local | Out-Null

# connect to Azure Subscription
 Connect-Azaccount

 Get-AzSubscription | Where-Object {$_.Name -eq $Subscription} | ForEach-Object {
    Write-Output "Switching to subscription '$($_.Name)' in TenantId '$($_.TenantId)'"
    $SubscriptionId = $_.Id
    Select-AzSubscription -TenantId $_.TenantId -SubscriptionId $SubscriptionId | Out-Null
}

# find unattached managed disks
 Get-AzDisk | Where-Object {$_.ManagedBy -eq $null} | Select Id

# find unattached NIC cards
 Get-AzNetworkInterface | Where-Object {$_.VirtualMachine -eq $null } | Select Id

# find unattached public-ips
 Get-AzPublicIpAddress | Where-Object {$_.IpConfiguration -eq $null } | Select Id

# find unattached unmanaged disks
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
