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
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

using Google.Protobuf;
using Azbackup.Proto;
using azpb = Azbackup.Proto;
using System.Text.RegularExpressions;

namespace Azbackup
{
    public class DirectoryJob
    {
        public string Source { get; set; }
        public string Destination { get; set; }

        [YamlMember(Alias = "attr-exclude", ApplyNamingConventions = false)]
        public string[] AttributeExclude { get; set; }

        [YamlMember(Alias = "attr-include", ApplyNamingConventions = false)]
        public string[] AttributeInclude { get; set;  }

        [YamlMember(Alias = "include-regex", ApplyNamingConventions = false)]
        public string[] IncludeRegex { get; set; }

        [YamlMember(Alias = "exclude-regex", ApplyNamingConventions = false)]
        public string[] ExcludeRegex { get; set; }
    }

    public class ContainerConfig
    {
        public string Container { get; set; }
    }

    public class Job
    {
        public string Name { get; set; }
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


    public class Schedule
    {
        [YamlMember(Alias = "days", ApplyNamingConventions = false)]
        public List<int> Days { get; set; }
        
        [YamlMember(Alias = "start", ApplyNamingConventions = false)]
        public int StartTime { get; set; }

        [YamlMember(Alias = "duration", ApplyNamingConventions = false)]
        public int Duration { get; set; }

        [YamlIgnore]
        public bool IsActive
        {
            get
            {
                if (Duration == 0)
                    return false;

                DateTime now = DateTime.Now;

                if (Days.Count > 0)
                {
                    // check to see this is a valid day
                    if (!Days.Contains( (int) now.DayOfWeek))
                        return false;
                }

                DateTime startTime = now.Date.AddMinutes(StartTime);
                DateTime endTime = startTime.AddMinutes(Duration);

                return (now >= startTime) && (now <= endTime);
            }
        }
    }


    public class Performance
    {
        [YamlMember(Alias = "upload-rate", ApplyNamingConventions = false)]
        public int UploadRate { get; set; }

        [YamlMember(Alias = "schedule", ApplyNamingConventions = false)]
        public List<Schedule> Schedules { get; set; }

        [YamlIgnore]
        public bool IsActive
        {
            get
            {
                foreach (var schedule in Schedules)
                {
                    if (!schedule.IsActive)
                        return false;
                }

                return true;
            }
        }


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
        

        private ManualResetEvent stopFlag = new ManualResetEvent(false);

        private CloudStorageAccount account;
        private CloudBlobClient client;
        private CloudBlobContainer archiveContainer;
        private CloudBlobContainer deltaContainer;
        private CloudBlobContainer historyContainer;

        private Dictionary<string, azpb.FileInfo> archiveFileData = new Dictionary<string, azpb.FileInfo>();
        private Dictionary<string, azpb.FileInfo> deltaFileData = new Dictionary<string, azpb.FileInfo>();

        private const string LastWriteTimeUTCKey   = "LastWriteTimeUTC";
        private const string LastWriteTimeUTCTicksKey = "LastWriteTimeUTCTicks";
        private const string ContentMD5Key = "ContentMD5";

        private static string GetConnectionString(string accountName, string accountKey)
        {
            return $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey}";
        }


        private NLog.Logger logger;


        public void Stop()
        {
            stopFlag.Set();
        }

  



        protected byte[] ComputeHash(string fullPath)
        {
            System.IO.FileInfo fi = new System.IO.FileInfo(fullPath);
            logger.Info("\tComputing hash for {0}...", fi.Name);
            using (MD5 md5 = MD5.Create())
            {
                using (FileStream fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    return md5.ComputeHash(fs);
                }
            }
        }


        public static string ConvertToHexString(byte[] bytes)
        {
            return bytes.Select(a => String.Format("{0:X2}", a)).Aggregate( (a, b) => a + b);
        }


        public void UploadDirectory(Job job, string rootDir, string currentDir, string destDir)
        {
            DirectoryInfo dirInfo = new DirectoryInfo(currentDir);

            string targetDir = dirInfo.FullName.Substring(rootDir.Length);
            
            string currentDestDir = destDir + targetDir.Replace('\\', '/') + ((targetDir.Length > 0) ? "/" : "");

            logger.Info("Processing directory: {0} to {1}", currentDir, currentDestDir);

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
            Mutex cacheBlockMutex = new Mutex();

            var fileInfos = dirInfo.EnumerateFiles();

            DirectoryJob currentDirJob = job.Directories.Single(a => a.Source == rootDir);

            bool attrSystemExclude = currentDirJob.AttributeExclude.Count(a => String.Compare(a.Substring(0, 1), "S", true) == 0) > 0;
            bool attrArchiveExclude = currentDirJob.AttributeExclude.Count(a => String.Compare(a.Substring(0, 1), "A", true) == 0) > 0;
            bool attrHiddenExclude = currentDirJob.AttributeExclude.Count(a => String.Compare(a.Substring(0, 1), "H", true) == 0) > 0;

            foreach (var fileInfo in fileInfos)
            {
                logger.Info("Processing file {0}", fileInfo.Name);

                bool includeFile = true;
                if (currentDirJob.IncludeRegex != null)
                {
                    includeFile = false;

                    foreach (string s in currentDirJob.IncludeRegex)
                    {
                        Regex regex = new Regex(s);
                        if (regex.IsMatch(fileInfo.FullName))
                        {
                            includeFile = true;
                            break;
                        }
                    }
                }

                if (currentDirJob.ExcludeRegex != null)
                {
                    foreach (string s in currentDirJob.ExcludeRegex)
                    {
                        Regex regex = new Regex(s);
                        if (regex.IsMatch(fileInfo.FullName))
                        {
                            includeFile = false;
                        }
                    }
                }

                bool attrInclude = true;

                if (attrSystemExclude && fileInfo.Attributes.HasFlag(FileAttributes.System))
                {
                    attrInclude = false;
                    logger.Info("\tSkipping due to System file attribute...");
                }

                if (attrHiddenExclude && fileInfo.Attributes.HasFlag(FileAttributes.Hidden))
                {

                    attrInclude = false;
                    logger.Info("\tSkipping due to Hidden file attribute...");
                }

                if (attrArchiveExclude && fileInfo.Attributes.HasFlag(FileAttributes.Archive))
                {
                    attrInclude = false;
                }

                if (!(attrInclude && includeFile))
                {
                    continue;
                }

                string destBlobName = destDir + fileInfo.FullName.Substring(rootDir.Length).Replace('\\', '/');

                bool storeArchive = false;
                bool storeDelta = false;
                bool updateDeltaTime = false;
                bool updateDeltaHash = false;
                bool copyHistory = false;
                bool updateArchiveCache = false;
                bool updateDeltaCache = false;

                byte[] md5Hash = null;

                bool checkDeltaStorage = false;

                if (archiveFileData.ContainsKey(fileInfo.FullName))
                {
                    if (archiveFileData[fileInfo.FullName].LastWriteUTCTicks != fileInfo.LastWriteTimeUtc.Ticks)
                    {
                        // check to see if there's an MD5 property
                        if (archiveFileData[fileInfo.FullName].Md5.Length > 0)
                        {
                            if (md5Hash == null)
                                md5Hash = ComputeHash(fileInfo.FullName);

                            if (archiveFileData[fileInfo.FullName].Md5.SequenceEqual(md5Hash))
                            {
                                updateArchiveCache = true;
                            }
                            else 
                            {
                                checkDeltaStorage = true;
                            }
                        }
                        else 
                        {
                            checkDeltaStorage = true;
                        }
                    }
                    else
                    {
                        // nothing to do
                    }
                }
                else
                {
                    var archiveBlobItem = archiveBlobItems.SingleOrDefault(a => a.Name == destBlobName);

                    if (archiveBlobItem == null)
                    {
                        md5Hash = ComputeHash(fileInfo.FullName);

                        storeArchive = true;
                    }
                    else
                    {
                        archiveBlobItem.FetchAttributes();
                        long lastWriteUTCTicks = long.Parse(archiveBlobItem.Metadata[LastWriteTimeUTCTicksKey]);
                        byte[] blobMd5Hash = Convert.FromBase64String(archiveBlobItem.Metadata[ContentMD5Key]);

                        if (lastWriteUTCTicks != fileInfo.LastWriteTimeUtc.Ticks)
                        {
                            // check the content
                            if (md5Hash == null)
                                md5Hash = ComputeHash(fileInfo.FullName);

                            if (!md5Hash.SequenceEqual(blobMd5Hash))
                            {
                                // the file has changed, so put it in delta storage
                                checkDeltaStorage = true;
                            }
                        }
                        else
                        {
                            // file times are equal, so store the hash in the cache
                            md5Hash = blobMd5Hash;
                            updateArchiveCache = true;
                        }

                    }
                }


                if (checkDeltaStorage)
                {
                    logger.Info("\tChecking delta storage container...");
                    if (deltaFileData.ContainsKey(fileInfo.FullName))
                    {
                        if (deltaFileData[fileInfo.FullName].LastWriteUTCTicks != fileInfo.LastWriteTimeUtc.Ticks)
                        {
                            // check the MD5
                            if (deltaFileData[fileInfo.FullName].Md5.Length > 0)
                            {
                                // check to see if the hashes are equal
                                if (md5Hash == null)
                                {
                                    md5Hash = ComputeHash(fileInfo.FullName);
                                }

                                if (md5Hash.SequenceEqual(deltaFileData[fileInfo.FullName].Md5))
                                {
                                    logger.Info("\tHashes are equal.  Updating time.");
                                    updateDeltaTime = true;
                                }
                                else
                                {
                                    logger.Info("\tHashes are not equal.  Storing to delta containter.");
                                    storeDelta = copyHistory = updateDeltaTime = updateDeltaHash = true;
                                }
                            }
                        }
                        else
                        {
                            // check to see if there's an MD5 time, if not compute it
                            if (deltaFileData[fileInfo.FullName].Md5.Length == 0)
                            {
                                if (md5Hash == null)
                                {
                                    md5Hash = ComputeHash(fileInfo.FullName);
                                }

                                updateDeltaCache = true;
                            }

                        }
                        
                    }
                    else
                    {
                        // no local metadata, so check the blob online
                        var deltaBlobItem = deltaBlobItems.SingleOrDefault(a => a.Name == destBlobName);
                        if (deltaBlobItem == null)
                        {
                            storeDelta = true;
                        }
                        else
                        {
                            // blob exists, so check it's file time
                            deltaBlobItem.FetchAttributes();
                            long deltaTimeTicks = long.Parse(deltaBlobItem.Metadata[LastWriteTimeUTCTicksKey]);
                            if (deltaTimeTicks != fileInfo.LastWriteTimeUtc.Ticks)
                            {
                                // file time are different, so check the MD5
                                if (deltaBlobItem.Metadata.ContainsKey(ContentMD5Key))
                                {
                                    byte[] blobMd5 = Convert.FromBase64String(deltaBlobItem.Metadata[ContentMD5Key]);

                                    if (md5Hash == null)
                                        md5Hash = ComputeHash(fileInfo.FullName);

                                    if (!blobMd5.SequenceEqual(md5Hash))
                                    {
                                        storeDelta = true;
                                        copyHistory = true;
                                    }
                                }
                                else
                                {
                                    // no hash, so store it to delta
                                    storeDelta = true;
                                    copyHistory = true;
                                }

                            }
                            else
                            {
                                logger.Info("\tStorage times are equal.");
                            }
                        }

                    }
                }

                if (storeArchive)
                {
                    logger.Info("\tUploading {0} to archive storage...", fileInfo.Name);

                    if (md5Hash == null)
                        md5Hash = ComputeHash(fileInfo.FullName);

                    UploadFile(fileInfo.FullName, archiveContainer, destBlobName, md5Hash, true);

                    if (stopFlag.WaitOne(0) || !YAMLConfig.Performance.IsActive)
                        return;

                    updateArchiveCache = true;
                }


                if (copyHistory)
                {
                    string historyBlobName = destBlobName + '.' + DateTime.UtcNow.ToString("yyyyMMddHHmmss");
                    CloudBlockBlob historyItem = historyContainer.GetBlockBlobReference(historyBlobName);
                    var deltaBlobItem = deltaBlobItems.SingleOrDefault(a => a.Name == destBlobName);
                    historyItem.StartCopy(deltaBlobItem);

                    logger.Info("\tCopying {0} to {1}...", deltaBlobItem.Name, historyBlobName);

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
                    if (md5Hash == null)
                        md5Hash = ComputeHash(fileInfo.FullName);

                    logger.Info("\tUploading {0} to delta storage...", fileInfo.Name);
                    UploadFile(fileInfo.FullName, deltaContainer, destBlobName, md5Hash);

                    if (stopFlag.WaitOne(0) || !YAMLConfig.Performance.IsActive)
                        return;

                    updateDeltaCache = true;
                }

                if (updateDeltaHash || updateDeltaTime)
                {
                    CloudBlockBlob deltaBlobItem = deltaBlobItems.Single(a => a.Name == destBlobName);
                    deltaBlobItem.FetchAttributes();

                    if (updateDeltaHash)
                    {
                        if (md5Hash == null)
                            md5Hash = ComputeHash(fileInfo.FullName);

                        deltaBlobItem.Metadata[ContentMD5Key] = Convert.ToBase64String(md5Hash);

                        logger.Info("\tUpdating delta blob hash...");
                    }

                    if (updateDeltaTime)
                    {
                        deltaBlobItem.Metadata[LastWriteTimeUTCTicksKey] = fileInfo.LastWriteTimeUtc.Ticks.ToString();

                        logger.Info("\tUpdating delta blob time...");
                    }

                    deltaBlobItem.SetMetadata();
                }

                if (updateArchiveCache)
                {
                    if (md5Hash == null)
                        md5Hash = ComputeHash(fileInfo.FullName);

                    azpb.FileInfo fi = new azpb.FileInfo()
                    {
                        Filename = fileInfo.FullName,
                        LastWriteUTCTicks = fileInfo.LastWriteTimeUtc.Ticks,
                        StorageTier = azpb.FileInfo.Types.StorageTier.Archive,
                        Md5 = ByteString.CopyFrom(md5Hash)
                    };

                    cacheBlock.DirInfo.FileInfos.Add(fi);

                    logger.Info("\tUpdating archive cache...");
                }

                if (updateDeltaCache)
                {
                    if (md5Hash == null)
                        md5Hash = ComputeHash(fileInfo.FullName);

                    azpb.FileInfo fi = new azpb.FileInfo()
                    {
                        Filename = fileInfo.FullName,
                        LastWriteUTCTicks = fileInfo.LastWriteTimeUtc.Ticks,
                        StorageTier = azpb.FileInfo.Types.StorageTier.Archive,
                        Md5 = ByteString.CopyFrom(md5Hash)
                    };

                    cacheBlock.DirInfo.FileInfos.Add(fi);

                    logger.Info("\tUpdating delta cache...");
                }

            }

            if (job.MetadataCacheFile != null && cacheBlock.DirInfo.FileInfos.Count > 0)
            {
                logger.Info("Writing {0} files to {1} cache...", cacheBlock.DirInfo.FileInfos.Count, job.MetadataCacheFile);
                using (FileStream fs = new FileStream(job.MetadataCacheFile, FileMode.Append, FileAccess.Write, FileShare.Read))
                {
                    cacheBlock.WriteDelimitedTo(fs);
                    fs.Flush();
                }
            }

            cacheBlock = null;

            var dirInfos = dirInfo.GetDirectories();
            foreach (var di2 in dirInfos)
            {
                if (stopFlag.WaitOne(0) || !YAMLConfig.Performance.IsActive)
                    return;

                UploadDirectory(job, rootDir, di2.FullName, destDir);
            }

        }



        public void UploadFile(string srcFilenameFullPath, CloudBlobContainer container, string destFilename, byte[] md5Hash, bool setArchiveFlag = false)
        {
            if (md5Hash == null)
                throw new ArgumentNullException("md5Hash");

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
                        if (stopFlag.WaitOne(0) || !YAMLConfig.Performance.IsActive)
                        {
                            return;
                        }

                        logger.Info("\tSending block {0} of {1}...", i+1, totalBlocks);

                        fs.Seek((long)i * BlockSize, SeekOrigin.Begin);
                        int bytesRead = fs.Read(buffer, 0, (int) BlockSize);

                        byte[] md5HashSegment = md5.ComputeHash(buffer, 0, bytesRead);

                        using (MemoryStream ms = new MemoryStream(buffer, 0, bytesRead))
                        {
                            sw.Restart();
                            blob.PutBlock(Convert.ToBase64String(BitConverter.GetBytes(i)), ms, Convert.ToBase64String(md5HashSegment) );
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
            blob.Metadata[LastWriteTimeUTCKey] = dateTimeString;
            blob.Metadata[LastWriteTimeUTCTicksKey] = fileInfo.LastWriteTimeUtc.Ticks.ToString();
            blob.Metadata[ContentMD5Key] = Convert.ToBase64String(md5Hash);
            blob.SetMetadata();

            if (setArchiveFlag)
            {
                blob.SetStandardBlobTier(StandardBlobTier.Archive);
            }
        }


        public void ExecuteJob(Job job)
        {
            var config = new NLog.Config.LoggingConfiguration();

            string logDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.Create),
                "azbackup_logs" );

            string logFileName = Path.Combine(logDir, "azbackup_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".log");

            var logFile = new NLog.Targets.FileTarget("logfile") { FileName = logFileName };
            config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, logFile);
            logFile.Layout = new NLog.Layouts.CsvLayout() { Layout = "${longdate},${level:uppercase=true},${message},${exception:format=tostring}" };

#if DEBUG
            var logConsole = new NLog.Targets.ColoredConsoleTarget("logconsole");
            logConsole.Layout = new NLog.Layouts.SimpleLayout() { Text = "${longdate}|${level:uppercase=true}|${message}|${exception:format=tostring}" };
            config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, logConsole);
#endif

            NLog.LogManager.Configuration = config;

            logger = NLog.LogManager.GetCurrentClassLogger();

            logger.Info("Starting job {0}...", job.Name);

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
            {
                logger.Error("No authentication key found.");
                throw new InvalidOperationException("No authenticiation key found.");
            }
                

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
                logger.Info("Found metadata cache file: {0}", job.MetadataCacheFile);

                // open the metadata file
                using (FileStream fs = new FileStream(job.MetadataCacheFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
                {
                    int counter = 0;

                    long lastGoodPos = 0;

                    try
                    {
                        while (true)
                        {
                            CacheBlock cacheBlock = CacheBlock.Parser.ParseDelimitedFrom(fs);

                            lastGoodPos = fs.Position;

                            foreach (var fileInfo in cacheBlock.DirInfo.FileInfos)
                            {
                                counter++;

                                string path = Path.Combine(cacheBlock.DirInfo.Directory, fileInfo.Filename);

                                switch (fileInfo.StorageTier)
                                {
                                case azpb.FileInfo.Types.StorageTier.Archive:
                                    archiveFileData[path] = fileInfo;
                                    break;

                                case azpb.FileInfo.Types.StorageTier.Delta:
                                    deltaFileData[path] = fileInfo;
                                    break;
                                }
                            }
                        }
                    }
                    catch (InvalidProtocolBufferException ipbe)
                    {
                        // we're done, so bail out
                        logger.Warn(ipbe, "Unable to parse protobuf.");
                        fs.SetLength(lastGoodPos);
                    }

                    logger.Info("Processed {0} files in metadata cache file.", counter);
                }
            }
            

            foreach (var dirJob in job.Directories)
            {

                DirectoryInfo di = new DirectoryInfo(dirJob.Source);

                string destDir = dirJob.Destination.Replace('\\', '/');
                if (!destDir.EndsWith("/"))
                    destDir += '/';

                UploadDirectory(job, di.FullName, di.FullName, destDir);

                if (stopFlag.WaitOne(0) || !YAMLConfig.Performance.IsActive)
                {
                    return;
                }
                    
            }
        }


    }

}
