<#
.DESCRIPTION
Checks for all currently installed Powershell modules whether they're outdated.
Optionally allows you to update the outdated modules as well.
Some modules may require Administrator permissions to update.

.LINK
original script by Jeff Hill:
https://sqladm.in/posts/check-if-you-have-old-versions-of-modules/
#>

Write-Progress -Activity "Checking Modules..." -CurrentOperation "Retrieving modules list..." -PercentComplete 0
# Get all the modules that are installed.
$Modules = Get-InstalledModule
# Create an empty array to store our results in.
$modcheck = @();
$i = 0;
# Loop through all the modules.
foreach($mod in $Modules){
    $i = $i + 1;
    Write-Progress -Activity "Checking Modules..." -CurrentOperation $mod.Name -PercentComplete ($i * 100 / $Modules.Count)
    # get the information for the latest from PSGallery.
	$PSGalleryMod = Find-Module $mod.Name;
    # Compare the installed version to the PSGallery version.
	if($PSGalleryMod.Version -ne $mod.version){
        # if they're different, put the details in a PSCustomObject.
		$modversions = [pscustomobject]@{
            Name = $($mod.name)
            InstalledVersion = $($mod.Version);InstalledPubDate =  $($mod.PublishedDate.tostring('MM/dd/yy'))
            AvailableVersion =  $($PSGalleryMod.Version)
            NewPubDate =  $($PSGalleryMod.PublishedDate.tostring('MM/dd/yy'))
        }
    # add the object to the array.
	$modcheck += $modversions;
	}
}

Write-Progress -Activity "Checking Modules..." -CurrentOperation $mod.Name -Completed

$modcheck | Format-Table

if ($modcheck.Count -eq 0) {
    Write-Host "No outdated modules found"
} else {
    $doUpdate = Read-Host -Prompt "Do you want to update all $($modcheck.Count) outdated module(s) now? (Y/N)"

    if ($doUpdate.ToUpper().StartsWith("Y"))
    {
        $i = 0;
        $modcheck | ForEach-Object {
            $i = $i + 1;
            Write-Progress -Activity "Updating Modules..." -CurrentOperation $_.Name -PercentComplete ($i * 100 / $modcheck.Count)
            Update-Module $_.Name
        }
    
        Write-Progress -Activity "Updating Modules..." -CurrentOperation $_.Name -Completed
    }
}