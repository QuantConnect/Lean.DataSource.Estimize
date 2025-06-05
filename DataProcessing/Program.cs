/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using QuantConnect.Configuration;
using QuantConnect.Logging;
using System;
using System.Diagnostics;
using System.IO;
using QuantConnect.Interfaces;
using QuantConnect.Util;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// Console program to download and write Estimize data to the NAS and cloud
    /// </summary>
    /// <remarks>
    /// This task is very inefficient in that it replaces every single raw file
    /// with an updated version every time we run the processing job, incurring
    /// high bandwidth usage on synchronization events. This is in part because
    /// of how Estimize implements their API (no date param). In conclusion, we
    /// write all of the data returned to disk, and then overwrite all raw data
    /// in the cloud, except for Estimize Consensus data.
    /// </remarks>
    public class Program
    {
        public static void Main()
        {
            var dataProvider
                = Composer.Instance.GetExportedValueByTypeName<IDataProvider>(Config.Get("data-provider", "DefaultDataProvider"));
            var mapFileResolver
                = Composer.Instance.GetExportedValueByTypeName<IMapFileProvider>(Config.Get("map-file-provider", "LocalZipMapFileProvider"));
            mapFileResolver.Initialize(dataProvider);

            var processingDateValue = Config.Get("processing-date", Environment.GetEnvironmentVariable("QC_DATAFLEET_DEPLOYMENT_DATE"));
            var processingDate = processingDateValue.IsNullOrEmpty() ? 
                DateTime.UtcNow.Date :
                Parse.DateTimeExact(processingDateValue, "yyyyMMdd");
            var date = processingDate.ToString("yyyy-MM-dd HH:mm:ss");
            
            var temporaryFolder = Config.Get("temp-output-directory", "/temp-output-directory");
            var tempEstimizeFolder = Path.Combine(temporaryFolder, "alternative", "estimize");

            // Makes sure we can download to the temp folder in a clean docker image
            Directory.CreateDirectory(tempEstimizeFolder);

            Log.Trace($"DataProcessing.Main(): Processing {date:yyyy-MM-dd}");

            var timer = Stopwatch.StartNew();

            // Appends "estimate" to the path we provide it
            var estimateDownloader = new EstimizeEstimateDataDownloader(tempEstimizeFolder, mapFileResolver);
            if (!estimateDownloader.Run(processingDate))
            {
                Log.Error($"DataProcessing.Main(): {date} - Failed to parse Estimate data");
                Environment.Exit(1);
            }

            timer.Stop();
            Log.Trace($"DataProcessing.Main(): {date} - Finished parsing Estimate data in {timer.Elapsed.TotalMinutes} minutes. Begin downloading Release data");
            timer.Restart();

            // Release data is required for the consensus downloader
            var releaseDownloader = new EstimizeReleaseDataDownloader(tempEstimizeFolder, mapFileResolver);
            if (!releaseDownloader.Run(out var infoByReleaseId))
            {
                Log.Error($"DataProcessing.Main(): {date} - Failed to parse Release data");
                Environment.Exit(1);
            }

            timer.Stop();
            Log.Trace($"DataProcessing.Main(): {date} - Finished parsing Release data in {timer.Elapsed.TotalMinutes} minutes. Begin downloading Consensus data");
            timer.Restart();

            // Consensus data relies on release data
            var consensusDownloader = new EstimizeConsensusDataDownloader(tempEstimizeFolder, new DirectoryInfo(Globals.DataFolder));
            if (!consensusDownloader.Run(infoByReleaseId))
            {
                Log.Error($"DataProcessing.Main(): {date} - Failed to parse Consensus data");
                Environment.Exit(1);
            }

            timer.Stop();
            Log.Trace($"DataProcessing.Main(): {date} - Finished parsing Consensus data in {timer.Elapsed.TotalMinutes} minutes");
            Environment.Exit(0);
        }
    }
}
