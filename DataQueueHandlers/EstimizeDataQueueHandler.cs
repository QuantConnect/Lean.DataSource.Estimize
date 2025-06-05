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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.DataProcessing;
using QuantConnect.Util;

namespace QuantConnect.DataSource.DataQueueHandlers
{
    /// <summary>
    /// Gathers Estimize data for use in live algorithms.
    /// The following types that can be returned from this DQH are:
    ///   * <see cref="EstimizeRelease"/>
    ///   * <see cref="EstimizeConsensus"/>
    ///   * <see cref="EstimizeEstimate"/>
    /// </summary>
    public class EstimizeDataQueueHandler : IDataQueueHandler
    {
        private const string _baseUrl = "https://api.estimize.com";

        private readonly Thread _thread;
        private readonly CancellationTokenSource _cancellationSource;
        private readonly JsonConverter _recentConsensusConverter;

        private readonly EstimizeDownloader _downloader;
        private readonly RateGate _dataQueueUpdateRateGate;
        private readonly RateGate _releaseQueryRateGate;
        private readonly bool _overrideSubscriptionCheck;

        // The following group are Estimize data sources that have
        // been emitted in the previous day. We should not emit
        // any data that has the same ID as the entries contained here.
        private readonly HashSet<string> _estimatesEmitted;
        // Consensus data we've already emitted. We should clear this
        // collection's entries older than 10 minutes since we only
        // query for data 6 minutes at a time.
        private List<EstimizeConsensus> _consensusesEmitted;

        private int _currentYear;
        private int _currentQuarter;
        private HashSet<Symbol> _universe;
        private HashSet<Symbol> _estimizeReleasesPendingSymbols;
        private readonly SortedList<string, PendingEstimizeRelease> _estimizeReleasesPending;

        private IDataAggregator _dataAggregator;
        private HashSet<Symbol> _subscriptionSymbols;
        private readonly object _subscriptionLock = new object();

        /// <summary>
        /// Creates an instance of an Estimize <see cref="IDataQueueHandler"/> implementation.
        /// A new background thread is immediately started on instantiation.
        /// </summary>
        /// <param name="dataAggregator">The data aggregator instance</param>
        /// <param name="overrideSubscriptionCheck">Overrides subscription checks</param>
        /// <param name="pollFrequencySeconds">Seconds to wait between polling the API for new data</param>
        public EstimizeDataQueueHandler()
        {
            _dataAggregator = Composer.Instance.GetPart<IDataAggregator>() ?? 
                Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(Config.Get("data-aggregator", "QuantConnect.Data.Common.CustomDataAggregator"));

            _subscriptionSymbols = new HashSet<Symbol>();
            _overrideSubscriptionCheck = Config.GetBool("estimize-override-subscription-check", true);

            _estimatesEmitted = new HashSet<string>();
            _consensusesEmitted = new List<EstimizeConsensus>();

            _universe = new HashSet<Symbol>();
            _estimizeReleasesPending = new SortedList<string, PendingEstimizeRelease>();
            _estimizeReleasesPendingSymbols = new HashSet<Symbol>();

            var pollFrequencySeconds = Config.GetInt("estimize-poll-frequency-seconds", 5);

            // Time to wait between polling events
            _dataQueueUpdateRateGate = new RateGate(1, TimeSpan.FromSeconds(pollFrequencySeconds));
            _releaseQueryRateGate = new RateGate(1, TimeSpan.FromMilliseconds(100));
            _downloader = new EstimizeDownloader();

            _recentConsensusConverter = new EstimizeRecentConsensusesConverter();

            _cancellationSource = new CancellationTokenSource();
            _thread = new Thread(() => Run())
            {
                Name = $"EstimizeDQH-{Guid.NewGuid()}",
                IsBackground = true
            };

            _thread.Start();
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            lock (_subscriptionLock)
            {
                if (_subscriptionSymbols.Add(dataConfig.Symbol))
                {
                    Log.Trace($"EstimizeDataQueueHandler.Subscribe(): {dataConfig.Symbol}");
                }
            }
            return _dataAggregator.Add(dataConfig, newDataAvailableHandler);
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            lock (_subscriptionLock)
            {
                if (_subscriptionSymbols.Remove(dataConfig.Symbol))
                {
                    _dataAggregator.Remove(dataConfig);
                    Log.Trace($"EstimizeDataQueueHandler.Unsubscribe(): {dataConfig.Symbol}");
                }
            }
        }

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
        }

        /// <summary>
        /// Returns whether the data provider is connected
        /// </summary>
        /// <returns>True if the data provider is connected</returns>
        public bool IsConnected => true;

        /// <summary>
        /// Determines if the provided Symbol is a valid subscription candidate
        /// </summary>
        /// <param name="symbol">Symbol</param>
        /// <returns>True for able to subscribe</returns>
        private bool CanSubscribe(Symbol symbol)
        {
            // Allow overriding of subscriptions
            if (_overrideSubscriptionCheck)
            {
                return true;
            }

            // ignore unsupported security types and universe symbols
            return symbol.ID.SecurityType == SecurityType.Base && !symbol.Value.Contains("-UNIVERSE-");
        }

        /// <summary>
        /// Begins collecting data from the data source. This method is ran in a new thread.
        /// </summary>
        private void Run()
        {
            // Since Estimize data is timestamped in the timezone America/New_York (with UTC offsets)
            // and Estimize serves data to us by interpreting the time in the NY Tz, we must adjust
            // the time we decide to query the API.
            // Note: they use America/New_York timezone which observes daylight savings time, not EST (UTC-05).
            var nextDay = DateTime.UtcNow.ConvertFromUtc(TimeZones.NewYork).Date.AddDays(1);
            var lastConsensusQuery = DateTime.MinValue;
            var currentTime = DateTime.UtcNow.ConvertFromUtc(TimeZones.NewYork);

            _currentYear = currentTime.Year;
            _currentQuarter = currentTime < new DateTime(currentTime.Year, 4, 1) ? 1 :
                currentTime < new DateTime(currentTime.Year, 7, 1) ? 2 :
                currentTime < new DateTime(currentTime.Year, 10, 1) ? 3 :
                4;

            // Get the most recent company and releases data, otherwise
            // consensuses will only update the next day.
            UpdateUniverse();
            UpdatePendingReleases();

            while (!_cancellationSource.IsCancellationRequested)
            {
                _dataQueueUpdateRateGate.WaitToProceed();

                currentTime = DateTime.UtcNow.ConvertFromUtc(TimeZones.NewYork);
                if (currentTime > nextDay)
                {
                    var yesterday = nextDay.AddDays(-1);

                    UpdateUniverse();
                    UpdateEstimates(yesterday, nextDay);

                    nextDay = nextDay.AddDays(1);

                    var previousYear = _currentYear;
                    var previousQuarter = _currentQuarter;

                    _currentYear = currentTime.Year;
                    _currentQuarter = currentTime < new DateTime(currentTime.Year, 4, 1) ? 1 :
                        currentTime < new DateTime(currentTime.Year, 7, 1) ? 2 :
                        currentTime < new DateTime(currentTime.Year, 10, 1) ? 3 :
                        4;

                    if (previousYear != _currentYear || previousQuarter != _currentQuarter)
                    {
                        // Once every quarter, we get the stocks that will have releases
                        // for the current new quarter. This saves us from having to query
                        // for releases every time the day changes since this operation is expensive
                        // and will take upwards of 5 minutes as of 2020-03-17
                        UpdatePendingReleases();
                    }

                    // Clear the data we've emitted since we'll be receiving new data
                    _estimatesEmitted.Clear();
                    continue;
                }

                var now = DateTime.UtcNow;
                if (now.Minute != lastConsensusQuery.Minute)
                {
                    // Update consensuses before the ready releases since
                    // we check the _pendingReleases collection for the status
                    // of a release before emitting a consensus
                    UpdateConsensuses(6);
                    UpdateReadyReleases(now.ConvertFromUtc(TimeZones.NewYork));
                }

                UpdateEstimates(currentTime, nextDay);
                lastConsensusQuery = now;
            }
        }

        /// <summary>
        /// Queries releases that have their <see cref="EstimizeRelease.ReleaseDate"/> in the past
        /// and emits any releases that have an EPS value.
        /// </summary>
        /// <param name="nowEastern">Current time in Eastern Time</param>
        private void UpdateReadyReleases(DateTime nowEastern)
        {
            var nowUtc = nowEastern.ConvertToUtc(TimeZones.NewYork);
            var releaseCandidates = _estimizeReleasesPending.Where(x => x.Value.NextTickTime <= nowUtc);
            var releasesToEmit = new List<EstimizeRelease>();

            foreach (var kvp in releaseCandidates)
            {
                var pendingRelease = kvp.Value;
                var url = $"{_baseUrl}/companies/{pendingRelease.Symbol.Value}/releases/{pendingRelease.FiscalYear}/{pendingRelease.FiscalQuarter}";
                var newRelease = Get<EstimizeRelease>(url);

                if (newRelease?.Eps == null)
                {
                    // Change the tick time and release date if Estimize updates it when we go to query it
                    if (newRelease != null && pendingRelease.ReleaseDate != newRelease.ReleaseDate)
                    {
                        pendingRelease.ReleaseDate = newRelease.ReleaseDate;
                        pendingRelease.NextTickTime = newRelease.ReleaseDate;
                        continue;
                    }

                    // Delay the wait on this point for another minute if null response or the release time
                    // hasn't changed on this point.
                    pendingRelease.NextTickTime = pendingRelease.NextTickTime.AddMinutes(1);
                    continue;
                }

                newRelease.Symbol = pendingRelease.Symbol;
                releasesToEmit.Add(newRelease);
            }

            foreach (var estimizeRelease in releasesToEmit)
            {
                _dataAggregator.Update(estimizeRelease);
            }

            foreach (var emitted in releasesToEmit)
            {
                _estimizeReleasesPending.Remove(emitted.Id);
            }

            // Get rid of any release that hasn't been released after 14 days (this might mean
            // that the ticker was delisted)
            var erroredReleases = _estimizeReleasesPending.Where(x => (nowUtc - x.Value.ReleaseDate) >= TimeSpan.FromDays(14)).ToList();
            foreach (var error in erroredReleases)
            {
                _estimizeReleasesPending.Remove(error.Key);
            }

            _estimizeReleasesPendingSymbols = _estimizeReleasesPending.Select(kvp => kvp.Value.Symbol).ToHashSet();
        }

        /// <summary>
        /// Updates the list of companies we can query for releases
        /// </summary>
        private void UpdateUniverse()
        {
            var url = $"{_baseUrl}/companies";
            var companies = Get<List<EstimizeDataDownloader.Company>>(url);
            if (companies == null)
            {
                Log.Error($"EstimizeDataQueueHandler.UpdateUniverse(): Companies request failed");
                return;
            }

            var symbols = new HashSet<Symbol>();
            foreach (var company in companies)
            {
                try
                {
                    var underlyingEquity = Symbol.Create(EstimizeDataDownloader.NormalizeTicker(company.Ticker), SecurityType.Equity, Market.USA);
                    var releaseSymbol = Symbol.CreateBase(typeof(EstimizeRelease), underlyingEquity, Market.USA);
                    symbols.Add(releaseSymbol);
                }
                catch (ArgumentException err)
                {
                    Log.Error(err, $"Failed to parse company {company.Ticker} as Symbol");
                }
            }

            // Only grab the Symbols in both collections
            _universe = symbols;
        }

        /// <summary>
        /// Gets this ticker's release dates
        /// </summary>
        private void UpdatePendingReleases()
        {
            Log.Trace("EstimizeDataQueueHandler.UpdatePendingReleases(): Start");

            var symbols = new List<Symbol>();
            lock (_subscriptionLock)
            {
                symbols = _universe.Where(s => (_overrideSubscriptionCheck || _subscriptionSymbols.Contains(s)) && !_estimizeReleasesPendingSymbols.Contains(s))
                    .ToList();
            }

            foreach (var symbol in symbols)
            {
                if (_cancellationSource.IsCancellationRequested)
                {
                    break;
                }

                _releaseQueryRateGate.WaitToProceed();

                var url = $"{_baseUrl}/companies/{symbol.Value}/releases";
                var nowUtc = DateTime.UtcNow;

                var releases = Get<List<EstimizeRelease>>(url);
                if (releases == null)
                {
                    Log.Error($"EstimizeDataQueueHandler.UpdatePendingReleases(): Release request failed for {symbol} (Q{_currentQuarter} {_currentYear})");
                    continue;
                }

                // We only want releases that meet the following conditions:
                //   1. Null EPS (no release published)
                //   2. The Release Date is at most 14 days before now (no stale points)
                //   3. It is not in the _estimizeReleasesPending collection (stops duplicates)
                //
                // We queue up not-yet announced earnings releases into the _estimizeReleasesPending
                // collection once at the change of every quarter since this method call is expensive.
                // We also make sure that this call happens at midnight Eastern Time (NY Tz) since that
                // is when Estimize considers their data to be in a new day.
                // We check that the release didn't already happen 14 days before now,
                // but it should already have been queued up. This is to prevent stale points, i.e. maybe a ticker was
                // delisted and the Release will never be updated.
                // When we call UpdateReadyReleases(...), the releases that were queued up here are
                // queried for again, and we check to see if there exists any earnings for them. If not,
                // they remain in the _estimizeReleasesPending queue for up to 14 days, and are removed from the collection
                // if they exceed that limit.
                foreach (var release in releases.Where(x => x.Eps == null && (nowUtc - x.ReleaseDate) <= TimeSpan.FromDays(14)))
                {
                    _estimizeReleasesPendingSymbols.Add(release.Symbol);

                    // If we get an update for a release we've previously cached, let's overwrite it
                    // since we now have more information about the release date.
                    PendingEstimizeRelease existingPendingRelease;
                    if (!_estimizeReleasesPending.TryGetValue(release.Id, out existingPendingRelease) ||
                        release.ReleaseDate != existingPendingRelease.ReleaseDate)
                    {
                        _estimizeReleasesPending[release.Id] = new PendingEstimizeRelease
                        {
                            Id = release.Id,
                            FiscalYear = release.FiscalYear,
                            FiscalQuarter = release.FiscalQuarter,
                            ReleaseDate = release.ReleaseDate,
                            NextTickTime = release.ReleaseDate,
                            Symbol = symbol
                        };
                        continue;
                    }
                }
            }

            Log.Trace("EstimizeDataQueueHandler.UpdatePendingReleases(): End");
        }

        /// <summary>
        /// Gets any new consensuses for the subscribed Symbols
        /// </summary>
        /// <param name="minutes">Minutes before now to get consensus data for</param>
        private void UpdateConsensuses(int minutes)
        {
            // The minimum number of minutes Estimize accepts is 6 minutes
            var within = Math.Max(6, Math.Min(1440, minutes));
            var url = $"{_baseUrl}/consensuses/recently_updated?within={within}";
            var consensuses = Get<List<EstimizeConsensus>>(url, _recentConsensusConverter);
            if (consensuses == null)
            {
                Log.Error("EstimizeDataQueueHandler.UpdateConsensuses(): Recent consensuses request failed");
                return;
            }

            var finalConsensuses = new List<EstimizeConsensus>();
            var newConsensuses = consensuses.Where(x => !_consensusesEmitted.Any(c => x.Id == c.Id && x.UpdatedAt == c.UpdatedAt && x.Source == c.Source)).ToList();

            if (newConsensuses.Count != 0)
            {
                lock (_subscriptionLock)
                {
                    newConsensuses = newConsensuses.Where(x => _overrideSubscriptionCheck || _subscriptionSymbols.Contains(x.Symbol)).ToList();
                }
            }

            foreach (var newConsensus in newConsensuses)
            {
                // Since the consensus object only contains the ID as a joinable field,
                // we must find any pending releases and discover the Symbol, FY and FQ
                // for the given consensus, since we're not provided this data in the response.
                PendingEstimizeRelease pendingRelease;
                if (!_estimizeReleasesPending.TryGetValue(newConsensus.Id, out pendingRelease) || !CanSubscribe(pendingRelease.Symbol))
                {
                    continue;
                }

                newConsensus.Symbol = Symbol.CreateBase(typeof(EstimizeConsensus), pendingRelease.Symbol.Underlying, Market.USA);
                newConsensus.FiscalYear = pendingRelease.FiscalYear;
                newConsensus.FiscalQuarter = pendingRelease.FiscalQuarter;

                finalConsensuses.Add(newConsensus);
            }

            if (finalConsensuses.Count != 0)
            {
                foreach (var consensus in finalConsensuses)
                {
                    _dataAggregator.Update(consensus);
                    _consensusesEmitted.Add(consensus);
                }
            }

            var nowUtc = DateTime.UtcNow;
            _consensusesEmitted = _consensusesEmitted.Where(x => (nowUtc - x.UpdatedAt) <= TimeSpan.FromMinutes(minutes)).ToList();
        }

        /// <summary>
        /// Updates the <see cref="_subscriptionData"/> collection with new <see cref="EstimizeEstimate"/> instances
        /// </summary>
        /// <param name="startDate">Starting date</param>
        /// <param name="endDate">Ending date</param>
        private void UpdateEstimates(DateTime startDate, DateTime endDate)
        {
            var url = $"{_baseUrl}/estimates?start_date={startDate:yyyy-MM-dd}&end_date={endDate:yyyy-MM-dd}";
            var estimates = Get<List<EstimizeEstimate>>(url);
            if (estimates == null)
            {
                Log.Error($"EstimizeDataQueueHandler.UpdateEstimates(): Estimates (date range) request failed");
                return;
            }

            lock (_subscriptionLock)
            {
                var newEstimates = estimates.Where(x => !_estimatesEmitted.Contains(x.Id));
                var finalEstimates = new List<EstimizeEstimate>();

                foreach (var estimate in newEstimates)
                {
                    var tickerSymbol = Symbol.Create(EstimizeDataDownloader.NormalizeTicker(estimate.Ticker), SecurityType.Equity, Market.USA);
                    estimate.Symbol = Symbol.CreateBase(typeof(EstimizeEstimate), tickerSymbol, Market.USA);

                    if (CanSubscribe(estimate.Symbol) && (_overrideSubscriptionCheck || _subscriptionSymbols.Contains(estimate.Symbol)))
                    {
                        finalEstimates.Add(estimate);
                        _estimatesEmitted.Add(estimate.Id);
                    }
                }

                foreach (var finalEstimate in finalEstimates)
                {
                    _dataAggregator.Update(finalEstimate);
                }
            }
        }

        /// <summary>
        /// Gets data from the given URL and converts the data to the type provided.
        /// </summary>
        /// <typeparam name="T">Type to convert JSON response to</typeparam>
        /// <param name="url">URL to query (GET)</param>
        /// <param name="converter">Optional <see cref="JsonConverter"/> to use in deserialization</param>
        /// <returns>Deserialized instance of type provided by user</returns>
        private T Get<T>(string url, JsonConverter converter = null)
        {
            try
            {
                var data = _downloader.HttpRequester(url).Result;

                return converter == null ?
                    JsonConvert.DeserializeObject<T>(data, new JsonSerializerSettings
                    {
                        DateTimeZoneHandling = DateTimeZoneHandling.Utc
                    }) :
                    JsonConvert.DeserializeObject<T>(data, new JsonSerializerSettings
                    {
                        Converters = new[] { converter },
                        DateTimeZoneHandling = DateTimeZoneHandling.Utc
                    });
            }
            catch (Exception)
            {
                // The exception has been logged for us already
                return default(T);
            }
        }

        /// <summary>
        /// Shuts down the background thread and sets the
        /// cancellation token to a canceled state.
        /// </summary>
        public void Dispose()
        {
            _cancellationSource.Cancel();
            _thread.Join(TimeSpan.FromSeconds(30));
        }

        /// <summary>
        /// Dummy class used to access existing <see cref="EstimizeDataDownloader.HttpRequester(string)"/> method
        /// </summary>
        protected class EstimizeDownloader : EstimizeDataDownloader
        {
        }
    }
}
