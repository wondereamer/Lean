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

using System.Collections.Generic;
using NUnit.Framework;
using QuantConnect.Lean.Engine.DataFeeds.Transport;


namespace QuantConnect.Tests.Engine.DataFeeds.Transport
{
    [TestFixture]
    public class MongoDBSubscriptionStreamReaderTests
    {

        List<KeyValuePair<string, string>>  sourceInfo = new List<KeyValuePair<string, string>>();
        [SetUp]
        public void SetUp()
        {
            sourceInfo.Add(new KeyValuePair<string, string>("date", "19920101 00:00:00"));
            sourceInfo.Add(new KeyValuePair<string, string>("ticker", "000001 2S1"));
            sourceInfo.Add(new KeyValuePair<string, string>("market", "sz"));
            sourceInfo.Add(new KeyValuePair<string, string>("ticktype", "Trade"));
            sourceInfo.Add(new KeyValuePair<string, string>("resolution", "Daily"));
            sourceInfo.Add(new KeyValuePair<string, string>("PeriodStart", "19920102"));
            sourceInfo.Add(new KeyValuePair<string, string>("PeriodFinish", "19920209"));
        }

        [TestCase("localhost", 27017)]
        public void ReadLine(string ip, int port)
        {
            var lines = new List<string>();

            var remoteReader = MongoDBSubscriptionStreamReader.Create(ip, port, sourceInfo);
            while (!remoteReader.EndOfStream)
            {
                var line = remoteReader.ReadLine();
                lines.Add(line);
            }

            Assert.AreEqual(22, lines.Count);
        }

    }
}
