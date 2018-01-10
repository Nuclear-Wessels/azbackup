using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azbackup;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

using Google.Protobuf;


namespace azbackup
{
    public class DirectoryJob
    {
        public string Source { get; set; }
        public string Destination { get; set; }

        [YamlMember(Alias = "attr-exclude", ApplyNamingConventions = false)]
        public string AttributeExclude { get; set; }
    }

    public class ContainerConfig
    {
        public string Container { get; set; }
    }

    public class Job
    {
        public List<DirectoryJob> Directories { get; set; }
        public ContainerConfig Archive { get; set; }
        public ContainerConfig Delta { get; set; }
        public ContainerConfig History { get; set; }
        public int Priority { get; set; }

        [YamlMember(Alias ="storage-account", ApplyNamingConventions = false)]
        public string StorageAccount { get; set; }

        [YamlMember(Alias = "auth-key", ApplyNamingConventions = false)]
        public string AuthKey { get; set; }

        [YamlMember(Alias = "auth-key-file", ApplyNamingConventions = false)]
        public string AuthKeyFile { get; set; }

        [YamlMember(Alias = "metadata-cache-file", ApplyNamingConventions = false)]
        public string MetadataCacheFile { get; set; }

    }


    public class Performance
    {
        [YamlMember(Alias = "upload-rate", ApplyNamingConventions = false)]
        public int UploadRate { get; set; }

    }

    public class YAMLConfig
    {
        public Performance Performance { get; set; }
        public List<Job> Jobs { get; set; }
    }


    public class AuthKeyFile
    {
        [YamlMember(Alias = "auth-key", ApplyNamingConventions = false)]
        public string AuthKey { get; set; }
    }


    public class AzureBackup
    {
        public YAMLConfig YAMLConfig { get; set; }

        private CloudStorageAccount account;
        private CloudBlobClient client;
        private CloudBlobContainer archiveContainer;
        private CloudBlobContainer deltaContainer;
        private CloudBlobContainer historyContainer;


        private Dictionary<string, long> archiveFileData = new Dictionary<string, long>();
        private Dictionary<string, long> deltaFileData = new Dictionary<string, long>();


        private static string GetConnectionString(string accountName, string accountKey)
        {
            return $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey}";
        }


        public void UploadDirectory(Job job, string rootDir, string currentDir, string destDir)
        {
            DirectoryInfo dirInfo = new DirectoryInfo(currentDir);

            string targetDir = dirInfo.FullName.Substring(rootDir.Length);
            
            string currentDestDir = destDir + targetDir.Replace('\\', '/') + ((targetDir.Length > 0) ? "/" : "");

            Console.WriteLine("Processing directory: {0} to {1}", currentDir, currentDestDir);

            IEnumerable<CloudBlockBlob> archiveBlobItems = null;
            IEnumerable<CloudBlockBlob> deltaBlobItems = null;
            IEnumerable<CloudBlockBlob> historyBlobItems = null;
            
            if (archiveContainer != null)
            {
                archiveBlobItems = archiveContainer.ListBlobs(currentDestDir).OfType<CloudBlockBlob>().Select(a => a as CloudBlockBlob);
            }
            
            if (deltaContainer != null)
            {
                deltaBlobItems = deltaContainer.ListBlobs(currentDestDir).OfType<CloudBlockBlob>().Select(a => a as CloudBlockBlob);
            }

            if (historyContainer != null)
            {
                historyBlobItems = historyContainer.ListBlobs(currentDestDir).OfType<CloudBlockBlob>().Select(a => a as CloudBlockBlob);
            }

            CacheBlock cacheBlock = new CacheBlock();
            cacheBlock.DirInfo = new DirInfo() { Directory = currentDir };

            var fileInfos = dirInfo.EnumerateFiles();
            foreach (var fileInfo in fileInfos)
            {
                Console.WriteLine("Processing file {0}", fileInfo.Name);

                if (fileInfo.Attributes.HasFlag(FileAttributes.System))
                {
                    Console.WriteLine("\tSkipping due to System file attribute...");
                    continue;
                }
                    
                string destBlobName = destDir + fileInfo.FullName.Substring(rootDir.Length).Replace('\\', '/');

                bool storeArchive = false;
                bool storeDelta = false;
                bool copyHistory = false;

                if (archiveFileData.ContainsKey(fileInfo.FullName))
                {
                    if (archiveFileData[fileInfo.FullName] != fileInfo.LastWriteTimeUtc.Ticks)
                    {
                        if (deltaFileData.ContainsKey(fileInfo.FullName))
                        {
                            if (deltaFileData[fileInfo.FullName] != fileInfo.LastAccessTimeUtc.Ticks)
                            {
                                storeDelta = copyHistory = true;
                            }
                        }
                        else
                        {
                            storeDelta = true;
                        }
                    }
                }
                else
                {
                    var archiveBlobItem = archiveBlobItems.SingleOrDefault(a => a.Name == destBlobName);

                    if (archiveBlobItem == null)
                    {
                        storeArchive = true;
                    }
                    else
                    {
                        archiveBlobItem.FetchAttributes();
                        long lastWriteUTCTicks = long.Parse(archiveBlobItem.Metadata["LastWriteTimeUTCTicks"]);

                        if (lastWriteUTCTicks != fileInfo.LastWriteTimeUtc.Ticks)
                        {
                            // Archive container can't modify content, so we'll check the Delta blob
                            var deltaBlobItem = deltaBlobItems.SingleOrDefault(a => a.Name == destBlobName);
                            if (deltaBlobItem == null)
                            {
                                storeDelta = true;
                            }
                            else
                            {
                                storeDelta = true;
                                copyHistory = true;
                            }
                        }
                        else
                        {
                            Azbackup.FileInfo azFileInfo = new Azbackup.FileInfo()
                            {
                                Filename = fileInfo.Name,
                                StorageTier = Azbackup.FileInfo.Types.StorageTier.Archive,
                                LastWriteUTCTicks = fileInfo.LastWriteTimeUtc.Ticks
                            };

                            cacheBlock.DirInfo.FileInfos.Add(azFileInfo);
                        }

                    }
                }

                if (storeArchive)
                {
                    Console.WriteLine("\tUploading {0} to archive storage...", fileInfo.Name);
                    UploadFile(fileInfo.FullName, archiveContainer, destBlobName, true);

                    Azbackup.FileInfo azFileInfo = new Azbackup.FileInfo()
                    {
                        Filename = fileInfo.Name,
                        StorageTier = Azbackup.FileInfo.Types.StorageTier.Archive,
                        LastWriteUTCTicks = fileInfo.LastWriteTimeUtc.Ticks
                    };

                    cacheBlock.DirInfo.FileInfos.Add(azFileInfo);
                }

                if (copyHistory)
                {
                    string historyBlobName = destBlobName + '.' + DateTime.UtcNow.ToString("yyyyMMddHHmmss");
                    CloudBlockBlob historyItem = historyContainer.GetBlockBlobReference(historyBlobName);
                    var deltaBlobItem = deltaBlobItems.SingleOrDefault(a => a.Name == destBlobName);
                    historyItem.StartCopy(deltaBlobItem);

                    Console.WriteLine("\tCopying {0} to {1}...", deltaBlobItem.Name, historyBlobName);

                    bool done = false;
                    while (!done)
                    {
                        switch (historyItem.CopyState.Status)
                        {
                        case CopyStatus.Success:
                            done = true;
                            break;

                        case CopyStatus.Pending:
                            Thread.Sleep(100);
                            break;
                                        
                        case CopyStatus.Aborted:
                            done = true;
                            break;

                        case CopyStatus.Invalid:
                            done = true;
                            break;

                        case CopyStatus.Failed:
                            done = true;
                            break;
                        }
                    }
                }

                if (storeDelta)
                {
                    Console.WriteLine("\tUploading {0} to delta storage...", fileInfo.Name);
                    UploadFile(fileInfo.FullName, deltaContainer, destBlobName);

                    Azbackup.FileInfo azFileInfo = new Azbackup.FileInfo()
                    {
                        Filename = fileInfo.Name,
                        StorageTier = Azbackup.FileInfo.Types.StorageTier.Delta,
                        LastWriteUTCTicks = fileInfo.LastWriteTimeUtc.Ticks
                    };

                    cacheBlock.DirInfo.FileInfos.Add(azFileInfo);
                }
            }


            if (job.MetadataCacheFile != null && cacheBlock.DirInfo.FileInfos.Count > 0)
            {
                Console.WriteLine("Writing {0} files to {1} cache...", cacheBlock.DirInfo.FileInfos.Count, job.MetadataCacheFile);
                using (FileStream fs = new FileStream(job.MetadataCacheFile, FileMode.Append, FileAccess.Write, FileShare.Read))
                {
                    cacheBlock.WriteDelimitedTo(fs);
                }
            }

            cacheBlock = null;

            var dirInfos = dirInfo.GetDirectories();
            foreach (var di2 in dirInfos)
            {
                UploadDirectory(job, rootDir, di2.FullName, destDir);
            }

        }



        public void UploadFile(string srcFilenameFullPath, CloudBlobContainer container, string destFilename, bool setArchiveFlag = false)
        {
            System.IO.FileInfo fileInfo = new System.IO.FileInfo(srcFilenameFullPath);

            Uri uri = new Uri(container.Uri.AbsoluteUri + '/' + destFilename);
             
            CloudBlockBlob blob = container.GetBlockBlobReference(destFilename);

            if (fileInfo.Length > 4 * 1024 * 1024)
            {
                const long BlockSize = 1L * 1024L * 1024L;

                // Perform a block upload.
                // Check to see if there are any blocks uploaded.

                IEnumerable<ListBlockItem> blockList = new List<ListBlockItem>();

                try
                {
                    blockList = blob.DownloadBlockList(BlockListingFilter.Uncommitted);
                }
                catch (StorageException e)
                {
                    // no blocks uploaded? 
                }

                int lastCommittedBlock = -1;
                if (blockList.Count() > 0)
                    lastCommittedBlock = blockList.Select(a => BitConverter.ToInt32(Convert.FromBase64String(a.Name), 0))
                                                  .Max();

                int totalBlocks = (int) (fileInfo.Length / BlockSize);

                if (fileInfo.Length % BlockSize > 0)
                    totalBlocks++;

                const int BitsPerBlock = (int) BlockSize * 8;

                Stopwatch sw = new Stopwatch();

                using (FileStream fs = new FileStream(srcFilenameFullPath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    byte[] buffer = new byte[BlockSize];
                    MD5 md5 = MD5.Create();

                    for (int i=lastCommittedBlock+1; i < totalBlocks; i++)
                    {
                        Console.Write("\tSending block {0} of {1}\r", i, totalBlocks);

                        fs.Seek((long)i * BlockSize, SeekOrigin.Begin);
                        int bytesRead = fs.Read(buffer, 0, (int) BlockSize);

                        byte[] md5Hash = md5.ComputeHash(buffer, 0, bytesRead);

                        using (MemoryStream ms = new MemoryStream(buffer, 0, bytesRead))
                        {
                            sw.Restart();
                            blob.PutBlock(Convert.ToBase64String(BitConverter.GetBytes(i)), ms, Convert.ToBase64String(md5Hash) );
                            sw.Stop();

                            if (YAMLConfig.Performance?.UploadRate > 0)
                            {
                                float bitsPerSecond = (float)BitsPerBlock / ((float)sw.ElapsedMilliseconds / 1000.0f);
                                float targetBitsPerSecond = (float)YAMLConfig.Performance.UploadRate * 1000.0f;

                                float actualSeconds = (float)sw.ElapsedMilliseconds / 1000.0f;
                                float targetSeconds = (float)BitsPerBlock / targetBitsPerSecond;

                                float diff = targetSeconds - actualSeconds;
                                if (diff > 0)
                                {
                                    Thread.Sleep((int)(diff * 1000.0f));
                                }
                            }
                        }
                    }

                    Console.WriteLine();
                }

                List<string> blockNamesList = new List<string>();

                int[] blockNums = blockList.Select(a => BitConverter.ToInt32(Convert.FromBase64String(a.Name), 0)).ToArray();

                for (int i=0; i<totalBlocks; i++)
                {
                    blockNamesList.Add(Convert.ToBase64String(BitConverter.GetBytes(i)));
                }

                blob.PutBlockList(blockNamesList);
            }
            else
            {
                blob.UploadFromFile(srcFilenameFullPath);
            }

            string dateTimeString = fileInfo.LastWriteTimeUtc.ToString("yyyy-MM-dd-HH-mm-ss.fff");
            blob.Metadata["LastWriteTimeUTC"] = dateTimeString;
            blob.Metadata["LastWriteTimeUTCTicks"] = fileInfo.LastWriteTimeUtc.Ticks.ToString();
            blob.SetMetadata();

            if (setArchiveFlag)
            {
                blob.SetStandardBlobTier(StandardBlobTier.Archive);
            }

        }


        public void ExecuteJob(Job job)
        {
            string authKey;

            if (job.AuthKey != null)
                authKey = job.AuthKey;
            else if (job.AuthKeyFile != null)
            {
                var deserializer = new DeserializerBuilder()
                       .WithNamingConvention(new CamelCaseNamingConvention())
                       .IgnoreUnmatchedProperties()
                       .Build();

                StreamReader reader = new StreamReader(job.AuthKeyFile);
                AuthKeyFile authKeyFile = deserializer.Deserialize<AuthKeyFile>(reader);

                authKey = authKeyFile.AuthKey;
            }
            else
                throw new InvalidOperationException("No authenticiation key found.");

            account = CloudStorageAccount.Parse(GetConnectionString(job.StorageAccount, authKey));
            client = account.CreateCloudBlobClient();
            
            archiveContainer = client.GetContainerReference(job.Archive.Container);
            archiveContainer.CreateIfNotExists();

            deltaContainer = client.GetContainerReference(job.Delta.Container);
            deltaContainer.CreateIfNotExists();

            historyContainer = client.GetContainerReference(job.History.Container);
            historyContainer.CreateIfNotExists();

            
            if (job.MetadataCacheFile != null)
            {
                // open the metadata file
                using (FileStream fs = new FileStream(job.MetadataCacheFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Read))
                {
                    try
                    {
                        while (true)
                        {
                            CacheBlock cacheBlock = CacheBlock.Parser.ParseDelimitedFrom(fs);

                            foreach (var fileInfo in cacheBlock.DirInfo.FileInfos)
                            {
                                string path = Path.Combine(cacheBlock.DirInfo.Directory, fileInfo.Filename);

                                switch (fileInfo.StorageTier)
                                {
                                case Azbackup.FileInfo.Types.StorageTier.Archive:
                                    archiveFileData[path] = fileInfo.LastWriteUTCTicks;
                                    break;

                                case Azbackup.FileInfo.Types.StorageTier.Delta:
                                    deltaFileData[path] = fileInfo.LastWriteUTCTicks;
                                    break;
                                }
                            }
                        }
                    }
                    catch (InvalidProtocolBufferException)
                    {
                        // we're done, so bail out
                    }
                }
            }
            

            foreach (var dirJob in job.Directories)
            {
                DirectoryInfo di = new DirectoryInfo(dirJob.Source);

                string destDir = dirJob.Destination.Replace('\\', '/');
                if (!destDir.EndsWith("/"))
                    destDir += '/';

                UploadDirectory(job, di.FullName, di.FullName, destDir);
            }
        }


        


        static void Main(string[] args)
        {
            var deserializer = new DeserializerBuilder()
                                   .WithNamingConvention(new CamelCaseNamingConvention())
                                   .IgnoreUnmatchedProperties()
                                   .Build();

            StreamReader reader = new StreamReader("config.yml");

            YAMLConfig yamlConfig = null;

            try
            {
                yamlConfig = deserializer.Deserialize<YAMLConfig>(reader);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Environment.Exit(1);
            }

            AzureBackup azureBackup = new AzureBackup()
            {
                YAMLConfig = yamlConfig
            };

            List<Job> jobs = yamlConfig.Jobs.OrderBy(a => a.Priority).ToList();

            foreach (Job job in jobs)
            {
                azureBackup.ExecuteJob(job);
            }
        }


    }

}
