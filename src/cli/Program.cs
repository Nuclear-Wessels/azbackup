

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Azbackup
{

    public static class Bootstrap
    {
        public static void Main(string[] args)
        {
            string configFile = "..\\..\\config.yml";

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
                    return;
                }
            }

            AzureBackup azureBackup = new AzureBackup()
            {
                YAMLConfig = yamlConfig
            };

            List<Job> jobs = yamlConfig.Jobs.OrderBy(a => a.Priority).ToList();


            if (!yamlConfig.Performance.IsActive)
            {
                // hang around until there's something to do
                Thread.Sleep(1000);
            }
            else
            {
                // time to do stuff
                foreach (Job job in jobs)
                {
                    azureBackup.ExecuteJob(job);

                    if (!yamlConfig.Performance.IsActive)
                        break;
                }
            }

        }
    }

    
}