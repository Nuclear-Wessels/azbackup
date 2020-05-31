using CommandLine;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NLog;
using NLog.Config;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;

namespace NuclearWessels
{

    public class CommonOptions
    {
        [Option('c', "cs", Required = true)]
        public string ConnectionString { get; set; }

        [Option]
        public bool DryRun { get; set; }

        [Option]
        public string LogFile { get; set; }

        [Option]
        public int Duration { get; set; }

    }

    public class BackupRestoreCommonOptions : CommonOptions
    {
        [Option("pc", Required = true, HelpText = "Primary container name")]
        public string PrimaryContainer { get; set; }

        [Option("dc", HelpText = "Delta container name")]
        public string DeltaContainer { get; set; }

        [Option("hc", HelpText = "History container name")]
        public string HistoryContainer { get; set; }
    }


    [Verb("backup")]
    public class BackupOptions : BackupRestoreCommonOptions
    {
        [Option('d', "dir", Required = true, HelpText = "Local directory to be backed up.")]
        public string BackupDirectory { get; set; }

        [Option("pt", HelpText = "Storage tier for primary container.")]
        public StandardBlobTier? PrimaryTier { get; set; }

        [Option("dt", HelpText = "Storage tier for delta container")]
        public StandardBlobTier? DeltaTier { get; set; }

        [Option("ht", HelpText = "Storage tier for history container")]
        public StandardBlobTier? HistoryTier { get; set; }

        [Option(HelpText = "Upload bandwidth (kbits/second)")]
        public int? Bandwidth { get; set; }

    }


    [Verb("restore")]
    public class RestoreOptions : BackupRestoreCommonOptions
    {
        [Option('d', "dir", Required = true, HelpText = "Directory to restore files")]
        public string RestoreDirectory { get; set; }

        public bool Rehydrate { get; set; }
    }


    [Verb("set-tier")]
    public class SetTierOptions : CommonOptions
    {

    }



    public class Backup
    {
        public BackupOptions BackupOptions { get; protected set; }

        public Backup(BackupOptions options)
        {
            BackupOptions = options;

            if (options.LogFile != null)
            {
                var logFile = new NLog.Targets.FileTarget("logfile") { FileName = options.LogFile };
            }
        }

        private DirectoryInfo baseDir;


        private Logger log = LogManager.GetCurrentClassLogger();


        private CloudStorageAccount account;
        private CloudBlobClient client;
        private CloudBlobContainer primaryContainer;
        private CloudBlobContainer deltaContainer;
        private CloudBlobContainer historyContainer;

        private const string LastWriteMetadataKeyName = "LastWriteUTC";


        public int DoBackup()
        {
            baseDir = new DirectoryInfo(BackupOptions.BackupDirectory);

            if (!baseDir.Exists)
            {
                log.Fatal("Unable to find {dir}.", baseDir.FullName);
                return 1;
            }

            account = CloudStorageAccount.Parse(BackupOptions.ConnectionString);
            client = account.CreateCloudBlobClient();
            primaryContainer = client.GetContainerReference(BackupOptions.PrimaryContainer);
            primaryContainer.CreateIfNotExists();

            if (!String.IsNullOrEmpty(BackupOptions.DeltaContainer))
            {
                deltaContainer = client.GetContainerReference(BackupOptions.DeltaContainer);
                deltaContainer.CreateIfNotExists();
            }

            if (!String.IsNullOrWhiteSpace(BackupOptions.HistoryContainer))
            {
                historyContainer = client.GetContainerReference(BackupOptions.HistoryContainer);
                historyContainer.CreateIfNotExists();
            }

            return BackupDir(baseDir.FullName);
        }


        public int BackupDir(string dir)
        {
            log.Info("Backing up directory {dir}", dir);
            
            // enumerate the files
            DirectoryInfo di = new DirectoryInfo(dir);
            string dirPath;
            if (di.FullName == baseDir.FullName)
            {
                dirPath = "";
            }
            else
            {
                dirPath = di.FullName.Substring(baseDir.FullName.Length + 1);
            }

            string blobPrefix = dirPath.Replace('\\', '/');
            
            IEnumerable <FileInfo> fileInfos = di.EnumerateFiles();

            // get a list of blob entries with the specified prefix
            var blobs = primaryContainer.ListBlobs(blobPrefix).OfType<CloudBlockBlob>();

            // look for files to be backed up
            foreach (var file in fileInfos)
            {
                string blobName = blobPrefix + "/" + file.Name;

                CloudBlockBlob primaryBlob = primaryContainer.GetBlockBlobReference(blobName);
                if (!primaryBlob.Exists())
                {
                    // the blob doesn't exist, so upload it to primary
                    UploadFile(primaryContainer, file, blobName, BackupOptions.PrimaryTier);
                }
                else
                {
                    // the blob exists, so figure out if we have a change
                    if (!AreFilesDifferent( primaryBlob, file))
                    {
                        log.Info("Skipping {0}.  No change.", file.Name);
                    }
                    else
                    {
                        // the file has changed, so update it
                        switch (primaryBlob.Properties.StandardBlobTier)
                        {
                        case StandardBlobTier.Archive:
                            // try save the file to the delta container
                            if (deltaContainer == null)
                            {
                                log.Error("File is marked as Archive, but no delta container specified.  Skipping.");
                            }
                            else
                            {
                                // We have a delta container, so check to see if the file is in there
                                CloudBlockBlob deltaBlob = deltaContainer.GetBlockBlobReference(blobName);
                                if (!deltaBlob.Exists())
                                {
                                    // There's no file in the delta container, so we'll upload it
                                    log.Info("Uploading {0}/{1}...", blobName, BackupOptions.DeltaContainer);
                                    UploadFile(deltaContainer, file, blobName, BackupOptions.DeltaTier);
                                }
                                else if (AreFilesDifferent(deltaBlob, file))
                                {
                                    // We've checked the delta container for a difference, and there is one.
                                    // So now check to see if there's a history container.

                                    if (historyContainer != null)
                                    {
                                        // store the original file in the history container
                                        DateTime lastWriteTime = GetLastWriteTimeUTC(deltaBlob);

                                        string historyBlobName = blobName + "." + lastWriteTime.ToString("yyyyMMdd-HHmmss");

                                        CloudBlockBlob historyBlob = historyContainer.GetBlockBlobReference(historyBlobName);

                                        log.Info("Copying {0}/{1} to {2}/{3}...",
                                            BackupOptions.DeltaContainer, blobName,
                                            BackupOptions.HistoryContainer, historyBlobName);
                                        CopyFile(deltaBlob, historyBlob);
                                    }

                                    // update the delta blob
                                    log.Info("Uploading {0}/{1}...", BackupOptions.DeltaContainer, blobName);
                                    UploadFile(deltaContainer, file, blobName, BackupOptions.DeltaTier);
                                }
                                else
                                {
                                    log.Info("Skipping {0}.  No change.", file.Name);
                                }                                
                            }
                            break;

                        case StandardBlobTier.Hot:
                        case StandardBlobTier.Cool:
                            if (deltaContainer != null)
                            {
                                CloudBlockBlob deltaBlob = deltaContainer.GetBlockBlobReference(blobName);

                                if (!deltaBlob.Exists())
                                {
                                    log.Info("Uploading {0}/{1}...", BackupOptions.DeltaContainer, blobName);
                                    UploadFile(deltaContainer, file, blobName, BackupOptions.DeltaTier);
                                }
                                else if (AreFilesDifferent(deltaBlob, file))
                                {
                                    // The files are different, so check to see if we need to copy the original file to 
                                    // the history container.

                                    if (historyContainer != null)
                                    {
                                        // store the original file in the history container
                                        DateTime lastWriteTime = GetLastWriteTimeUTC(deltaBlob);

                                        string historyBlobName = blobName + "." + lastWriteTime.ToString("yyyyMMdd-HHmmss");

                                        CloudBlockBlob historyBlob = historyContainer.GetBlockBlobReference(historyBlobName);

                                        log.Info("Copying {0}/{1} to {2}/{3}...",
                                            BackupOptions.DeltaContainer, blobName, 
                                            BackupOptions.HistoryContainer, historyBlobName);
                                        CopyFile(deltaBlob, historyBlob);
                                    }

                                    // update the delta blob
                                    log.Info("Uploading {0}/{1}...", BackupOptions.DeltaContainer, blobName);
                                    UploadFile(deltaContainer, file, blobName, BackupOptions.DeltaTier);
                                }
                                else
                                {
                                    log.Info("Skipping {0}.  No change.", file.Name);
                                }
                            }
                            else
                            {
                                // No delta container, so check to see if we want to store original files in the history 
                                // container
                                if (historyContainer != null)
                                {
                                    // first copy the existing file to the history container.
                                    DateTime lastModifiedTime = GetLastWriteTimeUTC(primaryBlob);

                                    string historyBlobName = blobName + "." + lastModifiedTime.ToString("yyyyMMdd-HHmmss");
                                    CloudBlockBlob historyBlob = historyContainer.GetBlockBlobReference(historyBlobName);
                                    log.Info("Copy file {0} to history container: {1}", blobName, historyContainer.Name);
                                    CopyFile(primaryBlob, historyBlob);
                                }

                                log.Info("Upload to {0}/{1}...", BackupOptions.PrimaryContainer, blobName);
                                UploadFile(primaryContainer, file, blobName, BackupOptions.PrimaryTier);
                            }
                            break;
                        }

                    }

                }
            }

            // look for files to be deleted


            IEnumerable<DirectoryInfo> dirs = di.EnumerateDirectories();

            foreach (DirectoryInfo di2 in dirs.OrderBy(a => a.Name) )
            {
                BackupDir(di2.FullName);
            }

            return 0;
        }


        public void CopyFile(CloudBlockBlob sourceBlob, CloudBlockBlob destBlob)
        {
            string leaseId = destBlob.StartCopy(sourceBlob);

            if (BackupOptions.DryRun)
            {
                return;
            }

            bool done = false;

            while (!done)
            {
                destBlob.FetchAttributes();

                switch (destBlob.CopyState.Status)
                {
                case CopyStatus.Aborted:
                    throw new InvalidOperationException("Copy unexpectedly aborted.");
                    
                case CopyStatus.Failed:
                case CopyStatus.Invalid:
                    throw new InvalidOperationException("Copy operation failed or invalid.");

                case CopyStatus.Pending:
                    // nothing to do.  Just hang out.
                    break;

                case CopyStatus.Success:
                    done = true;
                    break;
                }
            }
        }


        protected bool AreFilesDifferent(CloudBlockBlob blob, FileInfo fileInfo)
        {
            var blobMetadata = blob.Metadata;

            if (!blobMetadata.ContainsKey(LastWriteMetadataKeyName))
            {
                return true;
            }

            DateTime lastWriteUtc = DateTime.Parse(blob.Metadata[LastWriteMetadataKeyName]);

            long millisecondsDiff = (long)lastWriteUtc.Subtract(fileInfo.LastWriteTimeUtc).TotalMilliseconds;

            if (Math.Abs(millisecondsDiff) < 2000 && fileInfo.Length == blob.Properties.Length)
            {
                return false;
            }
            else
            {
                return true;
            }
        }


        protected static DateTime GetLastWriteTimeUTC(CloudBlockBlob blob)
        {
            var blobMetadata = blob.Metadata;
            DateTime lastWriteUtc = DateTime.Parse(blob.Metadata[LastWriteMetadataKeyName]);

            return lastWriteUtc;
        }



        public void UploadFile(CloudBlobContainer container, FileInfo fileInfo, string blobName, StandardBlobTier? tier)
        {
            if (tier.HasValue)
            {
                log.Info("Uploading file {0}/{1} ({2})", container.Name, blobName, tier.ToString());
            }
            else
            {
                log.Info("Uploading file {0}/{1}", container.Name, blobName);
            }

            if (BackupOptions.DryRun)
            {
                return;
            }

            const long FourMegabytes = 4 * 1024 * 1024;

            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);

            Stopwatch sw = new Stopwatch();


            if (fileInfo.Length <= FourMegabytes)
            {
                // just upload the file
                sw.Restart();
                blob.UploadFromFile(fileInfo.FullName);
                sw.Stop();

                if (BackupOptions.Bandwidth.HasValue)
                {
                    WaitBackup((int)fileInfo.Length, sw.Elapsed.TotalSeconds);
                }
            }
            else
            {
                // upload the file in parts

                // first check to see if there are parts already there
                IEnumerable<ListBlockItem> writtenBlockList;

                try
                {
                    writtenBlockList = blob.DownloadBlockList(BlockListingFilter.Uncommitted);
                }
                catch (StorageException)
                {
                    writtenBlockList = new List<ListBlockItem>();
                }

                List<long> blocksWritten = writtenBlockList.Select(a => BitConverter.ToInt64(Convert.FromBase64String(a.Name), 0)).ToList();

                const long BlockSize = 1024 * 1024;

                long numBlocks = fileInfo.Length / BlockSize;
                if (fileInfo.Length > numBlocks * BlockSize)
                {
                    numBlocks++;
                }

                List<long> blocksToBeWritten = new List<long>();

                for (long i=0; i<numBlocks; i++)
                {
                    blocksToBeWritten.Add(i);
                }

                blocksToBeWritten.RemoveAll(a => blocksWritten.Contains(a));

                byte[] buffer = new byte[BlockSize];

                long maxBlock = blocksToBeWritten.Max();


                using (FileStream fs = File.Open(fileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    foreach (long block in blocksToBeWritten)
                    {
                        log.Debug("Upload block {0}/{1} for {2}", block, maxBlock, blobName);

                        fs.Position = block * BlockSize;

                        int bytesToRead = (int) Math.Min(BlockSize, fileInfo.Length - block * BlockSize);
                        int bytesRead = fs.Read(buffer, 0, bytesToRead);
                        if (bytesRead != bytesToRead)
                        {
                            throw new InvalidOperationException();
                        }

                        string blockId = Convert.ToBase64String(BitConverter.GetBytes(block));

                        string md5Hash;

                        using (MD5 md5 = MD5.Create())
                        {
                            byte[] md5HashBytes = md5.ComputeHash(buffer, 0, bytesToRead);
                            md5Hash = Convert.ToBase64String(md5HashBytes);
                        }

                        
                        using (MemoryStream ms = new MemoryStream(buffer, 0, bytesToRead, false))
                        {
                            sw.Restart();
                            blob.PutBlock(blockId, ms, md5Hash);
                            sw.Stop();
                        }

                        if (BackupOptions.Bandwidth.HasValue)
                        {
                            WaitBackup(bytesToRead, sw.Elapsed.TotalSeconds);
                        }
                    }
                }

                List<string> allBlockIds = new List<string>();
                for (long i = 0; i < numBlocks; i++)
                {
                    string blockId = Convert.ToBase64String(BitConverter.GetBytes(i));
                    allBlockIds.Add(blockId);
                }

                blob.PutBlockList(allBlockIds);
            }

            blob.Metadata[LastWriteMetadataKeyName] = fileInfo.LastWriteTimeUtc.ToString("yyyy-MM-dd HH:mm:ss.fff");
            blob.SetMetadata();

            blob.FetchAttributes();

            if (tier.HasValue)
            {
                blob.SetStandardBlobTier(tier.Value);
            }

            log.Info("Finished uploading {0}", blobName);
        }


        protected void WaitBackup(int numBytes, double actualSeconds)
        {
            // calculate the time it was supposed to take according to the bandwidth
            double rateBytesPerSecond = (double)BackupOptions.Bandwidth.Value * 1000.0 / 8.0;

            double secondsAtBandwidth = (double)numBytes / rateBytesPerSecond;

            if (actualSeconds < secondsAtBandwidth)
            {
                double diff = secondsAtBandwidth - actualSeconds;
                Thread.Sleep((int)(diff * 1000.0));
            }
        }
    }


    public class Restore
    {

    }


    class AzBackup2
    {
        


        static int RunBackup(BackupOptions bo)
        {
            Backup backup = new Backup(bo);

            return backup.DoBackup();
        }


        static int RunRestore(RestoreOptions ro)
        {

            return 0;
        }


        static int Main(string[] args)
        {
            var config = new LoggingConfiguration();
            var consoleTarget = new ColoredConsoleTarget()
            {
                Name = "console",
                Layout = "${longdate}|${level:padding=-5:uppercase=true}|${message}"
            };

            config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, consoleTarget, "*");

            LogManager.Configuration = config;


            return CommandLine.Parser.Default.ParseArguments<BackupOptions, RestoreOptions>(args)
              .MapResult(
                (BackupOptions opts) => RunBackup(opts),
                (RestoreOptions opts) => RunRestore(opts),
                errs => 1);
        }
    }
}
