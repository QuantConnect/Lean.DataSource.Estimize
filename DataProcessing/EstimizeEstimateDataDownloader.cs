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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace QuantConnect.DataProcessing
{
    public class EstimizeEstimateDataDownloader : EstimizeDataDownloader
    {
        private readonly string _destinationFolder;

        /// <summary>
        /// Creates a new instance of <see cref="EstimizeEstimateDataDownloader"/>
        /// </summary>
        /// <param name="destinationFolder">The folder where the data will be saved</param>
        /// <param name="mapFileProvider">The map file provider instance to use</param>
        public EstimizeEstimateDataDownloader(string destinationFolder)
        {
            _destinationFolder = Path.Combine(destinationFolder, "estimate");
            Directory.CreateDirectory(_destinationFolder);
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run(DateTime date, bool patchData)
        {
            var stopwatch = Stopwatch.StartNew();
            var endDate = patchData ? DateTime.UtcNow : date;

            try
            {
                Log.Trace($"EstimizeEstimateDataDownloader.Run(): Start processing");

                var result = HttpRequester($"/estimates?start_date={date.AddDays(-5):yyyy-MM-dd}&end_date={endDate:yyyy-MM-dd}").Result;
                if (string.IsNullOrEmpty(result))
                {
                    Log.Trace($"EstimizeEstimateDataDownloader.Run(): No data received {stopwatch.Elapsed.ToStringInvariant(null)}");
                    return false;
                }

                JsonConvert.DeserializeObject<List<EstimizeEstimate>>(result, JsonSerializerSettings)
                    .GroupBy(estimate => GetMappedSymbol(estimate.Ticker, estimate.CreatedAt))
                    .Where(kvp => !string.IsNullOrEmpty(kvp.Key))
                    .DoForEach(kvp =>
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
                    });
            }
            catch (Exception e)
            {
                Log.Error(e);
                return false;
            }

            Log.Trace($"EstimizeEstimateDataDownloader.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");
            return true;
        }
    
        public bool ProcessHistoricalData(ref DateTime lastProcessedDate)
        {
            var stopwatch = Stopwatch.StartNew();

            var filename = Config.Get("estimize-historical-file", null);
            if (string.IsNullOrEmpty(filename) || !File.Exists(filename))
            {
                Log.Trace($"{GetType().Name}.ProcessHistoricalData(): No historical data file. Skipping...");
                return true;
            }

            var contentsBySymbol = new Dictionary<string, List<string>>();
            using var entryStream = Compression.Unzip(filename, "combined_estimates_new.csv", out var _);

            // estimate_id,eps,revenue,created_at,flagged,release_id,fiscal_quarter,fiscal_year,reported.eps,reported.revenue,reports_at,instrument_id,ticker,cusip,instrument_name,instrument_sector,instrument_industry,user_id,username,user_bio1,user_bio2,user_bio3,user_created_at,point_in_time_ticker,point_in_time_cusip,period_ends_on
            while (!entryStream.EndOfStream)
            {
                var line = entryStream.ReadLine();

                if (line.Count(x => x == ',') > 25)
                {
                    var instrumentName = line[line.IndexOf('\"')..(line.LastIndexOf('\"') + 1)];
                    line = line.Replace(instrumentName, string.Empty);
                }

                var csv = line.Split(',').Select(x => x.Trim()).ToArray();
                if (csv.Length < 23 || !char.IsDigit(csv[1], 0))
                {
                    continue;
                }

                var createdAt = Parse.DateTimeExact(csv[3], "yyyy-MM-ddTHH:mm:ssZ");
                if (createdAt.Year < 2011 || createdAt > DateTime.UtcNow)
                {
                    continue;
                }

                var mappedSymbol = GetMappedSymbol(
                    string.IsNullOrWhiteSpace(csv[^3]) ? csv[12] : csv[^3], 
                    createdAt);

                if (string.IsNullOrEmpty(mappedSymbol))
                {
                    continue;
                }

                if (!contentsBySymbol.TryGetValue(mappedSymbol, out var contents))
                {
                    contentsBySymbol[mappedSymbol] = contents = [];
                }
                contents.Add($"{createdAt.ToStringInvariant("yyyyMMdd HH:mm:ss")},{csv[0]},{csv[17]},{csv[18]},{csv[6]},{csv[7]},{csv[1]},{csv[2]},{csv[4]}");
                    
                if (createdAt > lastProcessedDate)
                {
                    lastProcessedDate = createdAt;
                }
            }
            

            foreach (var kvp in contentsBySymbol)
            {
                SaveContentToFile(_destinationFolder, kvp.Key, kvp.Value);
            }

            lastProcessedDate = lastProcessedDate.Date;
            Log.Trace($"ProcessHistoricalData.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");
            return true;
        }
    }
}