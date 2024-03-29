﻿/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using NUnit.Framework;
using QuantConnect.DataProcessing;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class EstimizeDownloaderTests
    {
        [TestCase("brk.b", "BRK-B")]
        [TestCase("googl", "GooGl")]
        [TestCase("enrnq", "enrnq - Defunct")]
        [TestCase("enrnq", "enrnq-defunct")]
        [TestCase("brk.b", "brk.b")]
        [TestCase("brk.a", "brk. a")]
        public void EstimizeDownloader_NormalizesShareClassTicker(string expectedTicker, string rawTicker)
        {
            var actualTicker = EstimizeDataDownloader.NormalizeTicker(rawTicker);
            Assert.AreEqual(expectedTicker, actualTicker);
        }
    }
}
