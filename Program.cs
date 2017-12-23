using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace azbackup
{
    public class ContainerConfig
    {
        public string Container { get; set; }
    }

    public class Job
    {
        public List<string> Directories { get; set; }
        public ContainerConfig Archive { get; set; }
        public int Priority { get; set; }

        [YamlMember(Alias ="storage-account", ApplyNamingConventions = false)]
        public string StorageAccount { get; set; }

    }

    public class YAMLConfig
    {
        public List<Job> Jobs { get; set; }
    }


    public class AzureDirUploader
    {
        public string AccountConnectionString { get; set; }

        public string ArchiveContainer { get; set; }
        public string DeltaContainer { get; set; }
        public string HistoryContainer { get; set; }


        public List<string> Directories { get; } = new List<string>();

        public int ParallelFiles { get; set; } = 1;


        private static string GetConnectionString(string accountName, string accountKey)
        {
            return $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey}";
        }

        private static CloudStorageAccount cloudAccount;
        private static CloudBlobClient cloudBlobClient;
        private static CloudBlobContainer cloudBlobContainer;


        /// <summary>
        /// Uploads files into the
        /// </summary>
        /// <param name="dir"></param>
        public void UploadArchive(string dir)
        {

        }



        public void UploadRootDirectory(string dir)
        {
            DirectoryInfo dirInfo = new DirectoryInfo(dir);

            string dirFullName = dirInfo.FullName;

            var fileInfos = dirInfo.EnumerateFiles();

            
        }


        public void UploadFiles(string dir)
        {
            //foreach (var fileInfo in fileInfos)
            //{
            //    //UploadFile(fileInfo.FullName, )
            //}
        }



        public void UploadFile(string srcFilenameFullPath, string destFilename, bool setArchiveFlag = false)
        {

        }


        static void Main(string[] args)
        {
            var deserializer = new DeserializerBuilder().WithNamingConvention(new CamelCaseNamingConvention()).IgnoreUnmatchedProperties().Build();

            StreamReader reader = new StreamReader("config.yml");

            YAMLConfig yamlConfig;

            try
            {
                yamlConfig = deserializer.Deserialize<YAMLConfig>(reader);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.InnerException);
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
