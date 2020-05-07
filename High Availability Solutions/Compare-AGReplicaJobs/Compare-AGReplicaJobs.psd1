#   This file is part of Compare-AGReplicaJobs.
#
#   Copyright 2020 Eitan Blumin <@EitanBlumin, https://www.eitanblumin.com>
#         while at Madeira Data Solutions <https://www.madeiradata.com>
#
#   Licensed under the MIT License (the "License");
# 
#   Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#   
#   The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#   
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.



#
# Module manifest for module 'Compare-AGReplicaJobs'
#

@{

# Version number of this module.
ModuleVersion = '1.0'

# ID used to uniquely identify this module
GUID = 'EB8975D0-9410-42C3-90C6-4B3C312C84BE'

# Author of this module
Author = 'Eitan Blumin (@EitanBlumin, https://www.eitanblumin.com)'

# Copyright statement for this module
Copyright = 'MIT License'

# Description of the functionality provided by this module
Description = 'PowerShell module file for importing all required modules for the Compare-AGReplicaJobs function.'

# Minimum version of the Windows PowerShell engine required by this module
PowerShellVersion = '3.0'

# Minimum version of the Windows PowerShell host required by this module
PowerShellHostVersion = '3.0'

# Script files (.ps1) that are run in the caller's environment prior to importing this module
ScriptsToProcess = @('Compare-AGReplicaJobs.ps1')

# Functions to export from this module
FunctionsToExport = 'Compare-AGReplicaJobs'

# HelpInfo URI of this module
# HelpInfoURI = ''

}