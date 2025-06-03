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
using QuantConnect.Data.Auxiliary;
using QuantConnect.DataSource;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace QuantConnect.DataProcessing
{
    public class EstimizeEstimateDataDownloader : EstimizeDataDownloader
    {
        private readonly string _destinationFolder;
        private readonly MapFileResolver _mapFileResolver;

        /// <summary>
        /// Creates a new instance of <see cref="EstimizeEstimateDataDownloader"/>
        /// </summary>
        /// <param name="destinationFolder">The folder where the data will be saved</param>
        /// <param name="mapFileProvider">The map file provider instance to use</param>
        public EstimizeEstimateDataDownloader(string destinationFolder, IMapFileProvider mapFileProvider)
        {
            _destinationFolder = Path.Combine(destinationFolder, "estimate");
            _mapFileResolver = mapFileProvider.Get(AuxiliaryDataKey.EquityUsa);

            Directory.CreateDirectory(_destinationFolder);
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public override bool Run(DateTime date)
        {
            var stopwatch = Stopwatch.StartNew();

            try
            {
                Log.Trace($"EstimizeEstimateDataDownloader.Run(): Start processing");

                var tasks = new List<Task>();
                // Makes sure we don't overrun Estimize rate limits accidentally
                IndexGate.WaitToProceed();

                tasks.Add(
                    // Extra days for redundancy
                    HttpRequester($"/estimates?start_date={date.AddDays(-5):yyyy-MM-dd}&end_date={date:yyyy-MM-dd}")
                        .ContinueWith(
                            y =>
                            {
                                if (y.IsFaulted)
                                {
                                    Log.Error($"EstimizeEstimateDataDownloader.Run(): Failed to get data");
                                    return;
                                }

                                var result = y.Result;
                                if (string.IsNullOrEmpty(result))
                                {
                                    Log.Trace($"EstimizeEstimateDataDownloader.Run(): No data received");
                                    return;
                                }

                                var estimates = JsonConvert.DeserializeObject<List<EstimizeEstimate>>(result, JsonSerializerSettings)
                                    .GroupBy(estimate =>
                                    {
                                        if (!TryNormalizeDefunctTicker(estimate.Ticker, out var ticker))
                                        {
                                            Log.Error($"EstimizeEstimateDataDownloader(): Defunct ticker {estimate.Ticker} is unable to be parsed. Continuing...");
                                            return string.Empty;
                                        }
                                        var normalizedTicker = NormalizeTicker(ticker);
                                        var oldTicker = normalizedTicker;
                                        var newTicker = normalizedTicker;
                                        var createdAt = estimate.CreatedAt;

                                        try
                                        {
                                            var mapFile = _mapFileResolver.ResolveMapFile(normalizedTicker, createdAt);

                                            // Ensure we're writing to the correct historical ticker
                                            if (!mapFile.Any())
                                            {
                                                Log.Trace($"EstimizeEstimateDataDownloader.Run(): Failed to find map file for: {newTicker} - on: {createdAt}");
                                                return string.Empty;
                                            }

                                            newTicker = mapFile.GetMappedSymbol(createdAt);
                                            if (string.IsNullOrWhiteSpace(newTicker))
                                            {
                                                Log.Trace($"EstimizeEstimateDataDownloader.Run(): New ticker is null. Old ticker: {oldTicker} - on: {createdAt.ToStringInvariant()}");
                                                return string.Empty;
                                            }

                                            if (!string.Equals(oldTicker, newTicker, StringComparison.InvariantCultureIgnoreCase))
                                            {
                                                Log.Trace($"EstimizeEstimateDataDownloader.Run(): Remapping {oldTicker} to {newTicker}");
                                            }
                                        }
                                        // We get a failure inside the map file constructor rarely. It tries
                                        // to access the last element of an empty list. Maybe this is a bug?
                                        catch (InvalidOperationException e)
                                        {
                                            Log.Error(e, $"EstimizeEstimateDataDownloader.Run(): Failed to load map file for: {oldTicker} - on {createdAt}");
                                            return string.Empty;
                                        }

                                        return newTicker;
                                    })
                                    .Where(kvp => !string.IsNullOrEmpty(kvp.Key));

                                foreach (var kvp in estimates)
                                {
                                    var csvContents = kvp.Select(x =>
                                        $"{x.CreatedAt.ToStringInvariant("yyyyMMdd HH:mm:ss")}," +
                                        $"{x.Id}," +
                                        $"{x.AnalystId}," +
                                        $"{x.UserName}," +
                                        $"{x.FiscalYear.ToStringInvariant()}," +
                                        $"{x.FiscalQuarter.ToStringInvariant()}," +
                                        $"{x.Eps.ToStringInvariant()}," +
                                        $"{x.Revenue.ToStringInvariant()}," +
                                        $"{x.Flagged.ToStringInvariant().ToLowerInvariant()}"
                                    );
                                    SaveContentToFile(_destinationFolder, kvp.Key, csvContents);
                                }
                            }
                        )
                    );

                Task.WaitAll(tasks.ToArray());
            }
            catch (Exception e)
            {
                Log.Error(e);
                return false;
            }

            Log.Trace($"EstimizeEstimateDataDownloader.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");
            return true;
        }
    }
}