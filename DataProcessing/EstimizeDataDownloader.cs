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
using QuantConnect.Data;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Util;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Policy;
using System.Threading.Tasks;

namespace QuantConnect.DataProcessing
{
    public abstract class EstimizeDataDownloader
    {
        private readonly string _clientKey = Config.Get("estimize-api-key");
        private readonly MapFileResolver _mapFileResolver;
        private readonly int _maxRetries = Config.GetInt("estimize-http-retries", 5);
        private readonly int _sleepMs = Config.GetInt("estimize-http-fail-sleep-ms", 1000);
        private static readonly List<char> _defunctDelimiters = [ '-', '_' ];

        /// <summary>
        /// Control the rate of download per unit of time.
        /// </summary>
        /// <remarks>
        /// Represents rate limits of 10 requests per 1.1 second
        /// </remarks>
        public RateGate IndexGate { get; } = new(10, TimeSpan.FromSeconds(1.1));

        protected readonly JsonSerializerSettings JsonSerializerSettings = new()
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };

        protected EstimizeDataDownloader(IMapFileProvider mapFileProvider)
        {
            _mapFileResolver = mapFileProvider.Get(AuxiliaryDataKey.EquityUsa);
        }

        public List<Company> GetCompanies()
        {
            try
            {
                var content = HttpRequester("/companies").Result;
                return [.. JsonConvert.DeserializeObject<List<Company>>(content).DistinctBy(x => x.Ticker)];
            }
            catch (Exception e)
            {
                Log.Error($"EstimizeDataDownloader.GetCompanies(): Error parsing companies list", e);    
            }
            return [];
        }

        public async Task<string> HttpRequester(string url)
        {
            for (var retries = 1; retries <= _maxRetries; retries++)
            {
                try
                {
                    using var client = new HttpClient();
                    client.BaseAddress = new Uri("https://api.estimize.com/");
                    client.DefaultRequestHeaders.Clear();

                    // You must supply your API key in the HTTP header X-Estimize-Key,
                    // otherwise you will receive a 403 Forbidden response
                    client.DefaultRequestHeaders.Add("X-Estimize-Key", _clientKey);

                    // Responses are in JSON: you need to specify the HTTP header Accept: application/json
                    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                    var response = await client.GetAsync(Uri.EscapeUriString(url));

                    if (response.StatusCode == HttpStatusCode.NotFound)
                    {
                        Log.Error($"EstimizeDataDownloader.HttpRequester(): File not found at url: {url}");
                        return string.Empty;
                    }

                    response.EnsureSuccessStatusCode();

                    return await response.Content.ReadAsStringAsync();
                }
                catch (Exception e)
                {
                    Log.Error(e, $"EstimizeDataDownloader.HttpRequester(): Error at HttpRequester. (retry {retries}/{_maxRetries})");
                    await Task.Delay(_sleepMs);
                }
            }

            throw new Exception($"Request failed with no more retries remaining (retry {_maxRetries}/{_maxRetries})");
        }

        /// <summary>
        /// Saves contents to disk, deleting existing zip files
        /// </summary>
        /// <param name="destinationFolder">Final destination of the data</param>
        /// <param name="ticker">Stock ticker</param>
        /// <param name="contents">Contents to write</param>
        protected void SaveContentToFile(string destinationFolder, string ticker, IEnumerable<string> contents)
        {
            ticker = ticker.ToLowerInvariant();
            var finalPath = new FileInfo(Path.Combine(destinationFolder, $"{ticker}.csv"));
            finalPath.Directory.Create();

            var lines = new HashSet<string>(contents);
            if (finalPath.Exists)
            {
                Log.Trace($"EstimizeDataDownloader.SaveContentToFile(): Adding to existing file: {finalPath}");
                foreach (var line in File.ReadAllLines(finalPath.FullName))
                {
                    lines.Add(line);
                }
            }
            else
            {
                Log.Trace($"EstimizeDataDownloader.SaveContentToFile(): Writing to file: {finalPath}");
            }

            var finalLines = lines.OrderBy(x => DateTime.ParseExact(x.Split(',').First(), "yyyyMMdd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal))
                .ToList();

            File.WriteAllLines(finalPath.FullName, finalLines);
        }

        /// <summary>
        /// Tries to normalize a potentially defunct ticker into a normal ticker.
        /// </summary>
        /// <param name="ticker">Ticker as received from Estimize</param>
        /// <param name="nonDefunctTicker">Set as the non-defunct ticker</param>
        /// <returns>true for success, false for failure</returns>
        public static bool TryNormalizeDefunctTicker(string ticker, out string nonDefunctTicker)
        {
            // The "defunct" indicator can be in any capitalization/case
            if (ticker.IndexOf("defunct", StringComparison.OrdinalIgnoreCase) > 0)
            {
                foreach (var delimChar in _defunctDelimiters)
                {
                    var length = ticker.IndexOf(delimChar);

                    // Continue until we exhaust all delimiters
                    if (length == -1)
                    {
                        continue;
                    }

                    nonDefunctTicker = ticker.Substring(0, length).Trim();
                    return true;
                }

                nonDefunctTicker = string.Empty;
                return false;
            }

            nonDefunctTicker = ticker;
            return true;
        }

        /// <summary>
        /// Normalizes Estimize tickers to a format usable by the <see cref="Data.Auxiliary.MapFileResolver"/>
        /// </summary>
        /// <param name="ticker">Ticker to normalize</param>
        /// <returns>Normalized ticker</returns>
        public static string NormalizeTicker(string ticker)
        {
            return ticker.ToLowerInvariant()
                .Replace("- defunct", string.Empty)
                .Replace("-defunct", string.Empty)
                .Replace(" ", string.Empty)
                .Replace("|", string.Empty)
                .Replace("-", ".");
        }

        public string GetMappedSymbol(string estimateTicker, DateTime createdAt)
        {
            if (!TryNormalizeDefunctTicker(estimateTicker, out var ticker))
            {
                Log.Error($"EstimizeDataDownloader.GetMappedSymbol(): Defunct ticker {estimateTicker} is unable to be parsed. Continuing...");
                return string.Empty;
            }
            var normalizedTicker = NormalizeTicker(ticker);
            var oldTicker = normalizedTicker;
            var newTicker = normalizedTicker;

            try
            {
                var mapFile = _mapFileResolver.ResolveMapFile(normalizedTicker, createdAt);

                // Ensure we're writing to the correct historical ticker
                if (!mapFile.Any())
                {
                    Log.Trace($"EstimizeDataDownloader.GetMappedSymbol(): Failed to find map file for: {newTicker} - on: {createdAt}");
                    return string.Empty;
                }

                newTicker = mapFile.GetMappedSymbol(createdAt);
                if (string.IsNullOrWhiteSpace(newTicker))
                {
                    Log.Trace($"EstimizeDataDownloader.GetMappedSymbol(): New ticker is null. Old ticker: {oldTicker} - on: {createdAt.ToStringInvariant()}");
                    return string.Empty;
                }

                if (!string.Equals(oldTicker, newTicker, StringComparison.InvariantCultureIgnoreCase))
                {
                    Log.Trace($"EstimizeDataDownloader.GetMappedSymbol(): Remapping {oldTicker} to {newTicker}");
                }
            }
            // We get a failure inside the map file constructor rarely. It tries
            // to access the last element of an empty list. Maybe this is a bug?
            catch (Exception e)
            {
                Log.Error(e, $"EstimizeDataDownloader.GetMappedSymbol(): Failed to load map file for: {oldTicker} - on {createdAt}");
                return string.Empty;
            }

            return newTicker;
        }

        public class Company
        {
            /// <summary>
            /// The name of the company
            /// </summary>
            [JsonProperty(PropertyName = "name")]
            public string Name { get; set; }

            /// <summary>
            /// The ticker/symbol for the company
            /// </summary>
            [JsonProperty(PropertyName = "ticker")]
            public string Ticker { get; set; }

            /// <summary>
            /// The CUSIP used to identify the security
            /// </summary>
            [JsonProperty(PropertyName = "cusip")]
            public string Cusip { get; set; }

            /// <summary>
            /// Returns a string that represents the Company object
            /// </summary>
            /// <returns></returns>
            public override string ToString() => $"{Cusip} - {Ticker} - {Name}";
        }
    }
}
