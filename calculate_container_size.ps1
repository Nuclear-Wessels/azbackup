# this script will show how to get the total size of the blobs in a container
# before running this, you need to create a storage account, create a container,
#    and upload some blobs into the container 
# note: this retrieves all of the blobs in the container in one command 
#       if you are going to run this against a container with a lot of blobs
#       (more than a couple hundred), use continuation tokens to retrieve
#       the list of blobs. We will be adding a sample showing that scenario in the future.

# these are for the storage account to be used
$resourceGroup = "backup"
$storageAccountName = "jaysbackup"
$containerName = "data-archive"

# get a reference to the storage account and the context
$storageAccount = Get-AzureRmStorageAccount `
  -ResourceGroupName $resourceGroup `
  -Name $storageAccountName
$ctx = $storageAccount.Context 

# get a list of all of the blobs in the container 
$listOfBLobs = Get-AzureStorageBlob -Container $ContainerName -Context $ctx 

# zero out our total
$length = 0

# this loops through the list of blobs and retrieves the length for each blob
#   and adds it to the total
$listOfBlobs | ForEach-Object { $length = $length + $_.Length }

# output the blobs and their sizes and the total 
Write-Host "List of Blobs and their size (length)"
Write-Host " " 
$listOfBlobs | select Name, Length
Write-Host " "
Write-Host "Total Length = " $length
