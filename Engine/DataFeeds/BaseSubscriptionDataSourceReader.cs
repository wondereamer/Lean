﻿/*
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
using QuantConnect.Data;
using System.ComponentModel;
using QuantConnect.Interfaces;
using System.Collections.Generic;
using QuantConnect.Lean.Engine.DataFeeds.Transport;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.Engine.DataFeeds
{
    /// <summary>
    /// A base class for implementations of the <see cref="ISubscriptionDataSourceReader"/>
    /// </summary>
    public abstract class BaseSubscriptionDataSourceReader : ISubscriptionDataSourceReader
    {
        /// <summary>
        /// True if we're in live mode, false for backtesting
        /// </summary>
        protected bool IsLiveMode { get; }

        /// <summary>
        /// The data cache provider to use
        /// </summary>
        protected IDataCacheProvider DataCacheProvider { get; }

        /// <summary>
        /// Event fired when the specified source is considered invalid, this may
        /// be from a missing file or failure to download a remote source
        /// </summary>
        public event EventHandler<InvalidSourceEventArgs> InvalidSource;

        /// <summary>
        /// Creates a new instance
        /// </summary>
        protected BaseSubscriptionDataSourceReader(IDataCacheProvider dataCacheProvider, bool isLiveMode)
        {
            DataCacheProvider = dataCacheProvider;
            IsLiveMode = isLiveMode;
        }

        /// <summary>
        /// Reads the specified <paramref name="source"/>
        /// </summary>
        /// <param name="source">The source to be read</param>
        /// <returns>An <see cref="IEnumerable{BaseData}"/> that contains the data in the source</returns>
        public abstract IEnumerable<BaseData> Read(SubscriptionDataSource source);

        /// <summary>
        /// Creates a new <see cref="IStreamReader"/> for the specified <paramref name="subscriptionDataSource"/>
        /// </summary>
        /// <param name="subscriptionDataSource">The source to produce an <see cref="IStreamReader"/> for</param>
        /// <returns>A new instance of <see cref="IStreamReader"/> to read the source, or null if there was an error</returns>
        protected IStreamReader CreateStreamReader(SubscriptionDataSource subscriptionDataSource)
        {
            IStreamReader reader = null;
            try
            {
                switch (subscriptionDataSource.TransportMedium)
                {
                    case SubscriptionTransportMedium.LocalFile:
                        reader = new LocalFileSubscriptionStreamReader(DataCacheProvider, subscriptionDataSource.Source);
                        break;

                    case SubscriptionTransportMedium.RemoteFile:
                        reader = HandleRemoteSourceFile(subscriptionDataSource);
                        break;

                    case SubscriptionTransportMedium.Rest:
                        reader = new RestSubscriptionStreamReader(subscriptionDataSource.Source, subscriptionDataSource.Headers, IsLiveMode);
                        break;

                    case SubscriptionTransportMedium.MongoDB:
                        reader = MongoDBSubscriptionStreamReader.Create(Config.Get("mongo-ip"), Config.GetInt("mongo-port"), subscriptionDataSource.Headers);
                        break;

                    default:
                        throw new InvalidEnumArgumentException("Unexpected SubscriptionTransportMedium specified: " + subscriptionDataSource.TransportMedium);
                }
            }
            catch (Exception e)
            {
                OnInvalidSource(subscriptionDataSource, e);
                return reader;
            }

            if (reader == null || reader.EndOfStream)
            {
                OnInvalidSource(subscriptionDataSource, new Exception($"The reader was empty for source: ${subscriptionDataSource.Source}"));
                return null;
            }
            return reader;
        }

        /// <summary>
        /// Event invocator for the <see cref="InvalidSource"/> event
        /// </summary>
        /// <param name="source">The <see cref="SubscriptionDataSource"/> that was invalid</param>
        /// <param name="exception">The exception if one was raised, otherwise null</param>
        protected void OnInvalidSource(SubscriptionDataSource source, Exception exception)
        {
            InvalidSource?.Invoke(this, new InvalidSourceEventArgs(source, exception));
        }

        /// <summary>
        /// Opens up an IStreamReader for a remote file source
        /// </summary>
        private IStreamReader HandleRemoteSourceFile(SubscriptionDataSource source)
        {
            SubscriptionDataSourceReader.CheckRemoteFileCache();

            try
            {
                // this will fire up a web client in order to download the 'source' file to the cache
                return new RemoteFileSubscriptionStreamReader(DataCacheProvider, source.Source, Globals.Cache, source.Headers);
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}
