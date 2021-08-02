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

namespace QuantConnect.DataSource.DataQueueHandlers
{
    /// <summary>
    /// Indicates a pending <see cref="EstimizeRelease"/> emit event
    /// </summary>
    public class PendingEstimizeRelease
    {
        /// <summary>
        /// Estimize Release ID
        /// </summary>
        [JsonProperty("id")]
        public string Id { get; set; }

        /// <summary>
        /// Estimize Release Fiscal Year
        /// </summary>
        [JsonProperty("fiscal_year")]
        public int FiscalYear { get; set; }

        /// <summary>
        /// Estimize Release Fiscal Quarter
        /// </summary>
        [JsonProperty("fiscal_quarter")]
        public int FiscalQuarter { get; set; }

        /// <summary>
        /// Time the Release is scheduled to be announced
        /// </summary>
        [JsonProperty("release_date")]
        public DateTime ReleaseDate { get; set; }

        /// <summary>
        /// Time we should re-query the API for the release
        /// </summary>
        [JsonIgnore]
        public DateTime NextTickTime { get; set; }

        /// <summary>
        /// Symbol of this pending release
        /// </summary>
        [JsonIgnore]
        public Symbol Symbol { get; set; }
    }
}
