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

using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace QuantConnect.DataSource.DataQueueHandlers
{
    /// <summary>
    /// Converts data received from the "recent consensuses" API
    /// call and converts it into a <see cref="EstimizeConsensus"/> object.
    /// </summary>
    /// <remarks>
    /// The "recent consensuses" API returns data in a different format
    /// than the schema defined in <see cref="EstimizeConsensus"/>.
    /// </remarks>
    public class EstimizeRecentConsensusesConverter : JsonConverter
    {
        /// <summary>
        /// Determines if we can serialize/deserialize the object type
        /// </summary>
        /// <param name="objectType">Type of the object</param>
        /// <returns></returns>
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(List<EstimizeConsensus>);
        }

        /// <summary>
        /// Converts the JSON into <see cref="EstimizeConsensus"/>
        /// </summary>
        /// <param name="reader">JSON reader</param>
        /// <param name="objectType">Object type</param>
        /// <param name="existingValue">Existing value</param>
        /// <param name="serializer">JSON Serializer</param>
        /// <returns><see cref="EstimizeConsensus"/></returns>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var token = JToken.ReadFrom(reader) as JArray;
            var parsedEntries = new List<EstimizeConsensus>();

            foreach (var entry in token)
            {
                entry["release_id"] = entry["id"] ?? entry["release_id"];
                entry["population"] = entry["source"];
                entry["metric"] = entry["type"];

                parsedEntries.Add(entry.ToObject<EstimizeConsensus>());
            }

            return parsedEntries;
        }

        /// <summary>
        /// Serialization implementation
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="value">Object to serialize</param>
        /// <param name="serializer">JSON serializer</param>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
}
