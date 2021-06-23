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
 *
*/

using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Orders;
using QuantConnect.Algorithm;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    /// <summary>
    /// Example algorithm using the custom data type as a source of alpha
    /// </summary>
    public class EstimizeConsensusesEstimatesReleasesDataAlgorithm : QCAlgorithm
    {
        private Symbol _customDataSymbol;
        private Symbol _equitySymbol;

        /// <summary>
        /// Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized.
        /// </summary>
        public override void Initialize()
        {
            SetStartDate(2020, 1, 1);  //Set Start Date
            SetEndDate(2021, 1, 1);    //Set End Date
            _equitySymbol = AddEquity("AAPL").Symbol;

            AddData<EstimizeConsensus>(_equitySymbol);
            AddData<EstimizeEstimate>(_equitySymbol);
            AddData<EstimizeRelease>(_equitySymbol);
        }

        /// <summary>
        /// OnData event is the primary entry point for your algorithm. Each new data point will be pumped in here.
        /// </summary>
        /// <param name="slice">Slice object keyed by symbol containing the stock data</param>
        public override void OnData(Slice slice)
        {
            PrintOutEstimizeData<EstimizeConsensus>(slice);
            PrintOutEstimizeData<EstimizeEstimate>(slice);
            PrintOutEstimizeData<EstimizeRelease>(slice);
        }

        private void PrintOutEstimizeData<T>(Slice data) 
            where T : IBaseData
        {
            var estimizeData = data.Get<T>();
            if (!estimizeData.IsNullOrEmpty()) 
            {
                foreach (var dataPoint in estimizeData.Values) 
                {
                    Log($"{typeof(T).Name}: {((T)(object)dataPoint)}");
                }
            }
        }

        /// <summary>
        /// Order fill event handler. On an order fill update the resulting information is passed to this method.
        /// </summary>
        /// <param name="orderEvent">Order event details containing details of the events</param>
        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            if (orderEvent.Status.IsFill())
            {
                Debug($"Purchased Stock: {orderEvent.Symbol}");
            }
        }
    }
}
