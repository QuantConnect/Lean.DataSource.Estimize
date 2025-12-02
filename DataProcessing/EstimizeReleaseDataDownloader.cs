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

using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.DataSource;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Util;
using RestSharp.Extensions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace QuantConnect.DataProcessing
{
    public class EstimizeReleaseDataDownloader : EstimizeDataDownloader
    {
        private readonly DirectoryInfo _destinationFolder;
        private readonly HashSet<string> _processTickers = [];

        /// <summary>
        /// Creates a new instance of <see cref="EstimizeReleaseDataDownloader"/>
        /// </summary>
        /// <param name="destinationFolder">The folder where the data will be saved</param>
        /// <param name="mapFileProvider">The map file provider instance to use</param>
        public EstimizeReleaseDataDownloader(string destinationFolder)
        {
            _destinationFolder = Directory.CreateDirectory(Path.Combine(destinationFolder, "release"));
            if (Config.TryGetValue("process-tickers", out string value) && value.HasValue())
            {
                _processTickers = [.. value.Split(",")];
            }
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run()
        {
            var stopwatch = Stopwatch.StartNew();

            if (_processTickers.IsNullOrEmpty())
            {
                GetCompanies().DoForEach(x =>
                    {
                        var ticker = GetMappedSymbol(x.Ticker, DateTime.UtcNow);
                        if (string.IsNullOrWhiteSpace(ticker)) return;
                        _processTickers.Add(ticker);
                    });

                _destinationFolder.Parent.CreateSubdirectory("estimate")
                    .EnumerateFiles("*.csv", SearchOption.AllDirectories)
                    .DoForEach(x => _processTickers.Add(x.Name[0..^4].ToUpper()));
            }
            try
            {
                var count = _processTickers.Count;
                var currentPercent = 0.05;
                var percent = 0.05;
                var i = 0;

                var fiscalYearQuarterByReleaseId = new List<string>();

                Log.Trace($"EstimizeReleaseDataDownloader.Run(): Start processing {count} companies");

                var tasks = new List<Task>();

                foreach (var ticker in _processTickers)
                {   
                    // Makes sure we don't overrun Estimize rate limits accidentally
                    IndexGate.WaitToProceed();

                    // Begin processing ticker with a normalized value
                    Log.Trace($"EstimizeReleaseDataDownloader.Run(): Processing {ticker}");

                    tasks.Add(
                        HttpRequester($"/companies/{ticker}/releases")
                            .ContinueWith(
                                y =>
                                {
                                    i++;

                                    if (y.IsFaulted)
                                    {
                                        Log.Error($"EstimizeReleaseDataDownloader.Run(): Failed to get data for {ticker}");
                                        return;
                                    }

                                    var result = y.Result;
                                    if (string.IsNullOrEmpty(result))
                                    {
                                        // We've already logged inside HttpRequester
                                        return;
                                    }

                                    // Just like TradingEconomics, we only want the events that already occured
                                    // instead of having "forecasts" that will change in the future taint our
                                    // data and make backtests non-deterministic. We want to have
                                    // consistency with our data in live trading historical requests as well
                                    var releases = JsonConvert.DeserializeObject<List<EstimizeRelease>>(result, JsonSerializerSettings)
                                        // Filter out anything before 2011 up to today
                                        .Where(x => x.ReleaseDate.Year >= 2011)
                                        .GroupBy(x => GetMappedSymbol(ticker, x.ReleaseDate))
                                        .Where(x => !string.IsNullOrEmpty(x.Key));

                                    foreach (var kvp in releases)
                                    {
                                        var csvContents = kvp.Select(x => $"{x.ReleaseDate.ToUniversalTime():yyyyMMdd HH:mm:ss},{x.Id},{x.FiscalYear},{x.FiscalQuarter},{x.Eps},{x.Revenue},{x.ConsensusEpsEstimate},{x.ConsensusRevenueEstimate},{x.WallStreetEpsEstimate},{x.WallStreetRevenueEstimate},{x.ConsensusWeightedEpsEstimate},{x.ConsensusWeightedRevenueEstimate}");
                                        SaveContentToFile(_destinationFolder.FullName, kvp.Key, csvContents);

                                        fiscalYearQuarterByReleaseId.AddRange(kvp.Select(x => $"{kvp.Key},{x.FiscalYear},{x.FiscalQuarter},{x.Id}"));
                                    }

                                    var percentDone = i / count;
                                    if (percentDone >= currentPercent)
                                    {
                                        Log.Trace($"EstimizeEstimateDataDownloader.Run(): {percentDone:P2} complete");
                                        currentPercent += percent;
                                    }
                                }
                            )
                    );
                }

                Task.WaitAll([.. tasks]);
            }
            catch (Exception e)
            {
                Log.Error(e);
                return false;
            }

            Log.Trace($"EstimizeReleaseDataDownloader.Run(): Finished in {stopwatch.Elapsed}");
            return true;
        }
    }
}