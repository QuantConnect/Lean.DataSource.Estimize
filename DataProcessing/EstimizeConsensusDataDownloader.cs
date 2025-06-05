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

using QuantConnect.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QuantConnect.Configuration;
using QuantConnect.DataSource;
using Type = QuantConnect.DataSource.EstimizeConsensus.ConsensusType;
using Source = QuantConnect.DataSource.EstimizeConsensus.ConsensusSource;
using QuantConnect.Util;

namespace QuantConnect.DataProcessing
{
    public class EstimizeConsensusDataDownloader : EstimizeDataDownloader
    {
        private readonly List<FileInfo> _releaseFiles;
        private readonly string _destinationFolder;
        private readonly DirectoryInfo _processedDataDirectory;
        private readonly HashSet<string> _processTickers;

        /// <summary>
        /// Creates a new instance of <see cref="EstimizeConsensusDataDownloader"/>
        /// </summary>
        /// <param name="destinationFolder">The folder where the data will be saved</param>
        /// <param name="processedDataDirectory">Processed data directory, the root path of where processed data lives</param>
        public EstimizeConsensusDataDownloader(string destinationFolder, DirectoryInfo processedDataDirectory = null)
        {
            var path = Path.Combine(destinationFolder, "release");
            var destinationReleaseDirectory = Directory.CreateDirectory(path);

            _processTickers = Config.Get("process-tickers", null)?.Split(",").ToHashSet();
            
            _releaseFiles = destinationReleaseDirectory.EnumerateFiles("*.csv", SearchOption.AllDirectories)
                .Where(x => !x.Name.StartsWith("."))
                .ToList();

            if (processedDataDirectory != null)
            {
                var processedReleasePath = Path.Combine(
                        processedDataDirectory.FullName,
                        "alternative",
                        "estimize",
                        "release");
                var processedReleaseDirectory = new DirectoryInfo(processedReleasePath);
                if (!processedReleaseDirectory.Exists)
                {
                    processedReleaseDirectory.Create();
                }

                _releaseFiles = _releaseFiles.Concat(
                        processedReleaseDirectory.GetFiles("*.csv", SearchOption.AllDirectories))
                    .Where(x => !x.Name.StartsWith("."))
                    .ToList();
            }

            _destinationFolder = Path.Combine(destinationFolder, "consensus");
            _processedDataDirectory = processedDataDirectory;
            
            Directory.CreateDirectory(_destinationFolder);
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run(HashSet<string> infoByReleaseId)
        {
            try
            {
                Log.Trace($"EstimizeConsensusDataDownloader.Run(): Start processing");

                var fiscalYearQuarterByReleaseId = infoByReleaseId
                    .Where(x => !x.Trim().IsNullOrEmpty())
                    .ToDictionary(x => x.Split(',')[0], x => x.Split(',').Skip(1).ToList());

                var tasks = new List<Task>();
                // Makes sure we don't overrun Estimize rate limits accidentally
                IndexGate.WaitToProceed();

                tasks.Add(
                    // Request the last 24 hours updated data
                    HttpRequester($"/consensuses/recently_updated?within=1440")
                        .ContinueWith(
                            y =>
                            {
                                if (y.IsFaulted)
                                {
                                    Log.Error($"EstimizeConsensusDataDownloader.Run(): Failed to get data");
                                    return;
                                }

                                var result = y.Result;
                                if (string.IsNullOrEmpty(result))
                                {
                                    Log.Trace($"EstimizeConsensusDataDownloader.Run(): No data received");
                                    return;
                                }

                                var consensuses = JsonConvert.DeserializeObject<List<EstimizeConsensus>>(result, JsonSerializerSettings);

                                foreach (var x in consensuses)
                                {
                                    if (x.Id.IsNullOrEmpty() || !fiscalYearQuarterByReleaseId.TryGetValue(x.Id, out var fiscalPeriodData))
                                    {
                                        Log.Trace($"EstimizeConsensusDataDownloader.Run(): Release data with ID {x.Id} is not found, skipping...");
                                        continue;
                                    }

                                    var ticker = fiscalPeriodData[0];
                                    x.FiscalYear = Convert.ToInt32(fiscalPeriodData[1]);
                                    x.FiscalQuarter = Convert.ToInt32(fiscalPeriodData[2]);
                                    var csvContents = new[] { $"{x.UpdatedAt.ToUniversalTime():yyyyMMdd HH:mm:ss},{x.Id},{x.Source},{x.Type},{x.Mean},{x.High},{x.Low},{x.StandardDeviation},{x.FiscalYear},{x.FiscalQuarter},{x.Count}" };
                                    SaveContentToFile(_destinationFolder, ticker, csvContents);
                                }
                            }
                        )
                    );

                Task.WaitAll(tasks.ToArray());
            }
            catch (Exception e)
            {
                Log.Error(e, "EstimizeConsensusDataDownloader.Run(): Failure in consensus download");
                return false;
            }

            return true;
        }

        private static EstimizeConsensus CreateEstimizeConsensus(string line, string filePath, string processedConsensusFile)
        {
            try
            {
                return new EstimizeConsensus(line);
            }
            catch (Exception e)
            {
                Log.Error($"EstimizeConsensusDataDownloader.Run():: Invalid data: {line} Files: {filePath} or {processedConsensusFile}. Message: {e}. StackTrace: {e.StackTrace}");
                return null;
            }
        }

        private IEnumerable<EstimizeConsensus> Unpack(EstimizeRelease estimizeEstimate, Source source, Type type, JObject jObject)
        {
            var jToken = jObject[source.ToLower()][type.ToLower()];
            var revisionsJToken = jToken["revisions"];

            var consensuses = revisionsJToken == null
                ? new List<EstimizeConsensus>()
                : JsonConvert.DeserializeObject<List<EstimizeConsensus>>(revisionsJToken.ToString(), JsonSerializerSettings);

            consensuses.Add(JsonConvert.DeserializeObject<EstimizeConsensus>(jToken.ToString(), JsonSerializerSettings));

            foreach (var consensus in consensuses)
            {
                consensus.Id = estimizeEstimate.Id;
                consensus.FiscalYear = estimizeEstimate.FiscalYear;
                consensus.FiscalQuarter = estimizeEstimate.FiscalQuarter;
                consensus.Source = source;
                consensus.Type = type;
            }

            return consensuses.Where(x => x.UpdatedAt > DateTime.MinValue);
        }
    }
}
