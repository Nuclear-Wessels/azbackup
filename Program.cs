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

namespace azbackup
{
    public class DirectoryJob
    {
        public string Source { get; set; }
        public string Destination { get; set; }
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


        private Dictionary<string, long> fileData = new Dictionary<string, long>();


        private static string GetConnectionString(string accountName, string accountKey)
        {
            return $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey}";
        }


        public static bool CompareDateTimes(DateTime dt1, DateTime dt2)
        {
            bool ok = true;

            ok &= dt1.Year == dt2.Year;
            ok &= dt1.Month == dt2.Month;
            ok &= dt1.Day == dt2.Day;
            ok &= dt1.Hour == dt2.Hour;
            ok &= dt1.Minute == dt2.Minute;
            ok &= dt1.Second == dt2.Second;
            ok &= dt1.Millisecond == dt2.Millisecond;

            return ok;
        }

        public void UploadDirectory(string rootDir, string currentDir, string destDir)
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

                // check to see if there's an archive file
                var archiveBlobItem = archiveBlobItems.SingleOrDefault(a => a.Name == destBlobName);
                if (archiveBlobItem != null)
                {
                    archiveBlobItem.FetchAttributes();

                    DateTime lastWriteUTC = DateTime.ParseExact(archiveBlobItem.Metadata["LastWriteTimeUTC"], "yyyy-MM-dd-HH-mm-ss.fff", CultureInfo.InvariantCulture);
                    lastWriteUTC = DateTime.SpecifyKind(lastWriteUTC, DateTimeKind.Utc);

                    long lastWriteUTCTicks = long.Parse(archiveBlobItem.Metadata["LastWriteTimeUTCTicks"]);

                    if (lastWriteUTCTicks != fileInfo.LastWriteTimeUtc.Ticks)
                    {
                        // Archive container can't modify content, so we'll check the Delta blob
                        var deltaBlobItem = deltaBlobItems.SingleOrDefault(a => a.Name == destBlobName);
                        if (deltaBlobItem == null)
                        {
                            // there is no delta blob, so we'll upload it

                            UploadFile(fileInfo.FullName, deltaContainer, destBlobName);
                        }
                        else
                        {
                            // there is a blob, so we'll copy it to the history folder
                            string historyBlobName = destBlobName + '.' + DateTime.UtcNow.ToString("yyyyMMddHHmmss");
                            CloudBlockBlob historyItem = historyContainer.GetBlockBlobReference(historyBlobName);
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

                            // now upload the new file to the delta container
                            Console.WriteLine("\tUploading {0} to delta storage...", fileInfo.Name);
                            UploadFile(fileInfo.FullName, deltaContainer, destBlobName);
                        }
                    }

                }
                else
                {
                    Console.WriteLine("\tUploading {0} to archive storage...", fileInfo.Name);
                    UploadFile(fileInfo.FullName, archiveContainer, destBlobName, true);
                }
            }

            var dirInfos = dirInfo.GetDirectories();
            foreach (var di2 in dirInfos)
            {
                UploadDirectory(rootDir, di2.FullName, destDir);
            }

        }



        public void UploadFile(string srcFilenameFullPath, CloudBlobContainer container, string destFilename, bool setArchiveFlag = false)
        {
            FileInfo fileInfo = new FileInfo(srcFilenameFullPath);

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
                        Console.WriteLine("\tSending block {0} of {1}", i, totalBlocks);

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

            
            if ()
            

            foreach (var dirJob in job.Directories)
            {
                DirectoryInfo di = new DirectoryInfo(dirJob.Source);

                string destDir = dirJob.Destination.Replace('\\', '/');
                if (!destDir.EndsWith("/"))
                    destDir += '/';

                UploadDirectory(di.FullName, di.FullName, destDir);
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


        //static void Main(string[] args)
        //{
        //    //System.Net.ServicePointManager.DefaultConnectionLimit = 35;

        //    cloudAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=http;AccountName=" + ACCOUNTNAME + ";AccountKey=" + ACCOUNTKEY);
        //    if (cloudAccount != null)
        //    {
        //        cloudBlobClient = cloudAccount.CreateCloudBlobClient();
        //        cloudBlobContainer = cloudBlobClient.GetContainerReference(CONTAINER);
        //        cloudBlobContainer.CreateIfNotExist();
        //    }

        //    // Upload the file
        //    CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(CONTAINER + "/" + System.IO.Path.GetFileName(LOCALFILE));

        //    blobUpload.BeginUploadFromStream();
            

        //    BlobTransfer transferUpload = new BlobTransfer();
        //    transferUpload.TransferProgressChanged += new EventHandler<BlobTransfer.BlobTransferProgressChangedEventArgs>(transfer_TransferProgressChanged);
        //    transferUpload.TransferCompleted += new System.ComponentModel.AsyncCompletedEventHandler(transfer_TransferCompleted);
        //    transferUpload.UploadBlobAsync(blockBlob, LOCALFILE);

        //    Transferring = true;
        //    while (Transferring)
        //    {
        //        Console.ReadLine();
        //    }

        //    // Download the file
        //    //CloudBlob blobDownload = ContainerFileTransfer.GetBlobReference(CONTAINER + "/" + System.IO.Path.GetFileName(LOCALFILE));
        //    //BlobTransfer transferDownload = new BlobTransfer();
        //    //transferDownload.TransferProgressChanged += new EventHandler<BlobTransfer.BlobTransferProgressChangedEventArgs>(transfer_TransferProgressChanged);
        //    //transferDownload.TransferCompleted += new System.ComponentModel.AsyncCompletedEventHandler(transfer_TransferCompleted);
        //    //transferDownload.DownloadBlobAsync(blobDownload, LOCALFILE + ".copy");

        //    //Transferring = true;
        //    //while (Transferring)
        //    //{
        //    //    Console.ReadLine();
        //    //}
        //}

        //static void transfer_TransferCompleted(object sender, System.ComponentModel.AsyncCompletedEventArgs e)
        //{
        //    Transferring = false;
        //    Console.WriteLine("Transfer completed. Press any key to continue.");
        //}

        //static void transfer_TransferProgressChanged(object sender, BlobTransfer.BlobTransferProgressChangedEventArgs e)
        //{
        //    Console.WriteLine("Transfer progress percentage = " + e.ProgressPercentage + " - " + (e.Speed / 1024).ToString("N2") + "KB/s");
        //}
    }

}
