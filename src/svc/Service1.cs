using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Azbackup;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;


namespace Azbackup
{

    public partial class AzBackupSvc : ServiceBase
    {
        public string ConfigFile
        {
            get { return configFile; }
            set { configFile = value; }
        }

        public AzBackupSvc()
        {
            InitializeComponent();
        }


        private Thread thread;

        private ManualResetEvent stopFlag = new ManualResetEvent(false);
        private ManualResetEvent doneFlag = new ManualResetEvent(false);

        private string configFile;

        protected void RunBackup()
        {
            var deserializer = new DeserializerBuilder()
                       .WithNamingConvention(new CamelCaseNamingConvention())
                       .IgnoreUnmatchedProperties()
                       .Build();

            YAMLConfig yamlConfig = null;

            using (StreamReader reader = new StreamReader(configFile))
            {
                try
                {
                    yamlConfig = deserializer.Deserialize<YAMLConfig>(reader);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    doneFlag.Set();
                    return;
                }
            }

            AzureBackup azureBackup = new AzureBackup()
            {
                YAMLConfig = yamlConfig
            };

            List<Job> jobs = yamlConfig.Jobs.OrderBy(a => a.Priority).ToList();


            while (!stopFlag.WaitOne(0))
            {
                
                if (!yamlConfig.Performance.IsActive)
                {
                    // hang around until there's something to do
                    bool set = stopFlag.WaitOne(60000);
                    if (set)
                        break;
                }
                else
                {
                    // time to do stuff
                    foreach (Job job in jobs)
                    {
                        azureBackup.ExecuteJob(job);

                        bool set = stopFlag.WaitOne(0);

                        if (set || !yamlConfig.Performance.IsActive)
                            break;
                    }
                }
            }

            doneFlag.Set();
        }



        protected override void OnStart(string[] args)
        {
            configFile = args[0];

            thread = new Thread(new ThreadStart(this.RunBackup));
            thread.Start();
        }

        protected override void OnStop()
        {
            stopFlag.Set();

            doneFlag.WaitOne();
        }
    }
}
