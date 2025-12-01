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
using Newtonsoft.Json.Linq;
using QuantConnect.Configuration;
using QuantConnect.Data.Market;
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
using Source = QuantConnect.DataSource.EstimizeConsensus.ConsensusSource;
using Type = QuantConnect.DataSource.EstimizeConsensus.ConsensusType;

namespace QuantConnect.DataProcessing
{
    public class EstimizeConsensusDataDownloader : EstimizeDataDownloader
    {
        private readonly DirectoryInfo _destinationFolder;
        private readonly DirectoryInfo _releaseFolder;
        private readonly HashSet<string> _processTickers = [];

        /// <summary>
        /// Creates a new instance of <see cref="EstimizeConsensusDataDownloader"/>
        /// </summary>
        /// <param name="destinationFolder">The folder where the data will be saved</param>
        /// <param name="processedDataDirectory">Processed data directory, the root path of where processed data lives</param>
        public EstimizeConsensusDataDownloader(string destinationFolder, IMapFileProvider mapFileProvider)
            : base(mapFileProvider)
        {
            _destinationFolder = Directory.CreateDirectory(Path.Combine(destinationFolder, "consensus"));
            _releaseFolder = _destinationFolder.Parent.CreateSubdirectory("release");
            if (Config.TryGetValue("process-tickers", out string value) && value.HasValue())
            {
                _processTickers = [.. value.Split(",")];
            }
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run(bool patchData = false)
        {
            var releaseFiles = _releaseFolder.EnumerateFiles("*.csv", SearchOption.AllDirectories).Where(x => !x.Name.StartsWith('.')).ToHashSet();

            if (!_processTickers.IsNullOrEmpty())
            {
                releaseFiles.RemoveWhere(x => !_processTickers.Contains(x.Name[0..^4].ToUpper()));
            }

            var releaseInfo = new Dictionary<string, Tuple<string, int, int>>();

            foreach (var releaseFile in releaseFiles)
            {
                var releases = File.ReadLines(releaseFile.FullName).Select(line => new EstimizeRelease(line)).ToList();
                releases.DoForEach(x => releaseInfo[x.Id] = Tuple.Create(releaseFile.Name[0..^4], x.FiscalYear, x.FiscalQuarter));

                if (patchData) PatchMissingConsensusData(releaseFile, releases);
            }

            var result = HttpRequester($"/consensuses/recently_updated?within=1440").Result;
            if (string.IsNullOrEmpty(result))
            {
                Log.Trace($"EstimizeConsensusDataDownloader.Run(): No data received");
                return false;
            }

            JsonConvert.DeserializeObject<List<EstimizeConsensus>>(result, JsonSerializerSettings)
                .Where(x => DateTime.UtcNow > x.EndTime && releaseInfo.ContainsKey(x.Id))
                .GroupBy(x => releaseInfo[x.Id].Item1)
                .DoForEach(kvp =>
                {
                    var csvContents = kvp.Select(consensus =>
                    {
                        var info = releaseInfo[consensus.Id];
                        return $"{consensus.UpdatedAt.ToUniversalTime():yyyyMMdd HH:mm:ss}," +
                               $"{consensus.Id}," +
                               $"{consensus.Source}," +
                               $"{consensus.Type}," +
                               $"{consensus.Mean},{consensus.High},{consensus.Low},{consensus.StandardDeviation}," +
                               $"{info.Item2},{info.Item3},{consensus.Count}";
                    });
                    SaveContentToFile(_destinationFolder.FullName, kvp.Key, csvContents);
                });

            return true;
        }

        public bool ProcessHistoricalData()
        {
            var filename = Config.Get("estimize-historical-file", null);
            if (string.IsNullOrEmpty(filename) || !File.Exists(filename))
            {
                return true;
            }

            var stopwatch = Stopwatch.StartNew();
            var contentsBySymbol = new Dictionary<string, List<string>>();
            using var entryStream = Compression.Unzip(filename, "combined_consensus_new.csv", out _);

            //date,ticker,cusip,instrument_id,instrument_name,instrument_sector,instrument_industry,fiscal_year,fiscal_quarter,reports_at,
            //  estimize.eps.weighted,estimize.eps.high,estimize.eps.low,estimize.eps.sd,estimize.eps.count,
            //  estimize.revenue.weighted,estimize.revenue.high,estimize.revenue.low,estimize.revenue.sd,estimize.revenue.count,
            //wallstreet.eps,wallstreet.revenue,actual.eps,actual.revenue,release_id,point_in_time_ticker,point_in_time_cusip,period_ends_on
            while (!entryStream.EndOfStream)
            {
                var line = entryStream.ReadLine();

                if (line.Count(x => x == ',') > 27)
                {
                    var instrumentName = line[line.IndexOf('\"')..(line.LastIndexOf('\"') + 1)];
                    line = line.Replace(instrumentName, string.Empty);
                }

                var csv = line.Split(',').Select(x => x.Trim()).ToArray();
                if (csv.Length < 28 || !char.IsDigit(csv[9], 0))
                {
                    continue;
                }

                var reportsdAt = Parse.DateTimeExact(csv[9], "yyyy-MM-ddTHH:mm:sszzz", System.Globalization.DateTimeStyles.AdjustToUniversal);
                if (reportsdAt.Year < 2011 || reportsdAt > DateTime.UtcNow)
                {
                    continue;
                }

                var mappedSymbol = GetMappedSymbol(
                    string.IsNullOrWhiteSpace(csv[^3]) ? csv[1] : csv[^3],
                    reportsdAt);

                if (string.IsNullOrEmpty(mappedSymbol))
                {
                    continue;
                }

                if (!contentsBySymbol.TryGetValue(mappedSymbol, out var contents))
                {
                    contentsBySymbol[mappedSymbol] = contents = [];
                }

                var eventId = $"{reportsdAt.ToStringInvariant("yyyyMMdd HH:mm:ss")},{csv[^4]}";
                var quarter = $"{csv[7]},{csv[8]}";

                if (!string.IsNullOrWhiteSpace(csv[10]))
                {
                    contents.Add($"{eventId},Estimize,Eps,{csv[10]},{csv[11]},{csv[12]},{csv[13]},{quarter},{csv[14]}");
                }
                if (!string.IsNullOrWhiteSpace(csv[15]))
                {
                    contents.Add($"{eventId},Estimize,Revenue,{csv[15]},{csv[16]},{csv[17]},{csv[18]},{quarter},{csv[19]}");
                }
                if (!string.IsNullOrWhiteSpace(csv[20]))
                {
                    contents.Add($"{eventId},WallStreet,Eps,{csv[20]},,,,{quarter},");
                }
                if (!string.IsNullOrWhiteSpace(csv[21]))
                {
                    contents.Add($"{eventId},WallStreet,Revenue,{csv[21]},,,,{quarter},");
                }
            }

            foreach (var kvp in contentsBySymbol)
            {
                SaveContentToFile(_destinationFolder.FullName, kvp.Key, kvp.Value);
            }

            Log.Trace($"ProcessHistoricalData.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");
            return true;
        }

        private void PatchMissingConsensusData(FileInfo releaseFile, List<EstimizeRelease> releases)
        {
            if (releases.Count > 0)
            {
                var consensusFile = releaseFile.FullName.Replace("release", "consensus");
                if (File.Exists(consensusFile))
                {
                    var consensusIds = File.ReadLines(consensusFile).Select(line => line.Split(',')[1]).ToHashSet();
                    releases = [.. releases.Where(x => !consensusIds.Contains(x.Id))];
                }
            }

            if (releases.Count == 0)
            {
                return;
            }

            Log.Trace($"EstimizeConsensusDataDownloader.PatchMissingConsensusData(): Missing Consensus for {releaseFile.Name}: {releases.Count}");

            // Makes sure we don't overrun Estimize rate limits accidentally
            IndexGate.WaitToProceed();

            var tasks = releases
                .Select(x => HttpRequester($"/releases/{x.Id}/consensus")
                .ContinueWith(y =>
                {
                    if (y.IsFaulted)
                    {
                        Log.Error($"EstimizeConsensusDataDownloader.PatchMissingConsensusData(): Failed to get data for /releases/{x.Id}/consensus");
                        return [];
                    }

                    var result = y.Result;
                    if (string.IsNullOrEmpty(result))
                    {
                        return [];
                    }

                    var csvContents = new List<string>();

                    foreach (var (source, data) in JObject.Parse(result))
                    {
                        foreach (var (type, datum) in data as JObject)
                        {
                            var consensus = JsonConvert.DeserializeObject<EstimizeConsensus>(datum.ToString(), JsonSerializerSettings);
                            if (consensus == null || consensus.UpdatedAt.Year <= 2011 || consensus.UpdatedAt > DateTime.UtcNow)
                            {
                                continue;
                            }
                            csvContents.Add($"{consensus.UpdatedAt.ToUniversalTime():yyyyMMdd HH:mm:ss}," +
                                $"{x.Id}," +
                                $"{Enum.Parse<Source>(source, true)}," +
                                $"{Enum.Parse<Type>(type, true)}," +
                                $"{consensus.Mean},{consensus.High},{consensus.Low},{consensus.StandardDeviation}," +
                                $"{x.FiscalYear},{x.FiscalYear},{consensus.Count}");
                        }
                    }

                    return csvContents;
                })
            );
            Task.WaitAll([.. tasks]);

            var csvContents = tasks.SelectMany(x => x.Result).ToList();
            SaveContentToFile(_destinationFolder.FullName, releaseFile.Name[..^4], csvContents);
        }
    }
}