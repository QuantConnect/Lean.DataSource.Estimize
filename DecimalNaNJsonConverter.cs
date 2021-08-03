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

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Converts any NaN values found in the JSON object to null
    /// </summary>
    internal class DecimalNaNJsonConverter : JsonConverter 
    {
        private readonly bool _convertNaNToNull;

        public DecimalNaNJsonConverter()
        {
            _convertNaNToNull = true;
        }

        public DecimalNaNJsonConverter(bool convertNaNToNull = true)
        {
            _convertNaNToNull = convertNaNToNull;
        }

        public override bool CanConvert(Type _) 
        {
            return true;
        }

        public override void WriteJson(JsonWriter _, object __, JsonSerializer ___) 
        {
            throw new NotImplementedException("The DecimalNaNJsonConverter does not implement a WriteJson method");
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var parsedValue = JToken.Load(reader).Value<double?>();
            if (double.IsNaN(parsedValue ?? 0))
            {
                return _convertNaNToNull
                    ? null
                    : 0m;
            }

            return (decimal?)parsedValue;
        }
    }
}