using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;

using QuantConnect.Interfaces;
using QuantConnect.Logging;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Globalization;

namespace QuantConnect.Lean.Engine.DataFeeds.Transport
{

    public sealed class MongoAdapter {  
        private static IMongoDatabase _db;
        private static readonly object _dbLocker = new object();  

        public static IMongoDatabase GetDB(string ip, int port){  
            if (_db == null) {  
                lock(_dbLocker) {  
                    if (_db == null) {  
                        var client = new MongoClient($"mongodb://{ip}:{port}");
                        _db = client.GetDatabase("quant");
                    }  
                }  
            }  
            return _db;
        }  
    }  
    
    public class MongoDBSubscriptionStreamReader: IStreamReader
    {
        private static IMongoDatabase _db;

        private const double _scaleFactor = 10000;
        private readonly string _code;
        private readonly string _curDate;
        private readonly string _periodStart;
        private readonly string _periodFinish;
        private readonly Resolution _resolution;
        private readonly IMongoCollection<BsonDocument> _collection;
        private IEnumerator<string> enumerator;
        private string _next;
        private bool _endOfStream = true;

        /// <summary>
        /// Create a MongoDBSubscriptionStreamReaser
        /// It will connect mongoDB for once
        /// </summary>
        public static MongoDBSubscriptionStreamReader Create(string ip, int port,
            IReadOnlyList<KeyValuePair<string, string>> headers)
        {
            IMongoDatabase db;
            try {
                db = MongoAdapter.GetDB(ip, port);
            }
            catch (Exception)
            {
                Log.Error($"Connect mongodb failed: ip-{ip}, port-{port}");
                throw;
            }
            var reader = new MongoDBSubscriptionStreamReader(db, headers);
            reader.FetchData();
            return reader;
        }

        private MongoDBSubscriptionStreamReader(IMongoDatabase db, IReadOnlyList<KeyValuePair<string, string>> header)
        {
            var aHeader = header.ToDictionary((keyItem) => keyItem.Key, (valueItem) => valueItem.Value);
            _db = db;
            _resolution = (Resolution)Resolution.Parse(typeof(Resolution), aHeader["resolution"]);
            _collection = _db.GetCollection<BsonDocument>(GetCollectionName(aHeader, _resolution));
            _periodStart = aHeader["PeriodStart"];
            _periodFinish = aHeader["PeriodFinish"];
            _curDate = aHeader["date"];
            _code = $"{aHeader["ticker"].Split(" ")[0]}.{aHeader["market"]}".ToUpper();
        }

        private string GetCollectionName(Dictionary<string, string> aHeader, Resolution resolution)
        {
            string collectionName;
            if (_resolution == Resolution.Daily)
            {
                collectionName = $"{aHeader["market"]}_{aHeader["ticktype"]}BAR_{aHeader["resolution"]}".ToUpper();
            }
            else
            {
                throw new Exception($"resolution: '{aHeader["resolution"]}' not supported");
            }
            return collectionName;
        }

        public void FetchData()
        {
            // 如果用户直接改数据库，如果这里没有排序，就得不到保证.
            var filterBuilder = Builders<BsonDocument>.Filter;
            CultureInfo provider = CultureInfo.InvariantCulture;
            var filter = filterBuilder.Eq("code", _code) & filterBuilder.Gte("trade_date", DateTime.ParseExact(_periodStart, "yyyyMMdd", provider)) & filterBuilder.Lte("trade_date", DateTime.ParseExact(_periodFinish, "yyyyMMdd", provider));
            var sort = Builders<BsonDocument>.Sort.Ascending("trade_date");

            var cursor = _collection.Find(filter).ToCursor();
            enumerator = cursor.ToEnumerable().Select(x =>BsonToStringLine(x, _resolution)).GetEnumerator();
            _endOfStream = !enumerator.MoveNext();
            _next = enumerator.Current;
        }
        
        private string BsonToStringLine(BsonDocument doc, Resolution resolution)
        {
            string line = "";
            if (resolution == Resolution.Daily)
            {
                var open = doc["open"].AsDouble * _scaleFactor;
                var high = doc["high"].AsDouble * _scaleFactor;
                var low = doc["low"].AsDouble * _scaleFactor;
                var close = doc["close"].AsDouble * _scaleFactor;
                var time = DateTime.Parse(doc["trade_date"].ToString());
                line = $"{time.ToString("yyyyMMdd")} 00:00,{open},{high},{low},{close},{doc["volume"]}";
            }
            return line;
        }

        /// <summary>
        /// Gets <see cref="SubscriptionTransportMedium.LocalFile"/>
        /// </summary>
        public SubscriptionTransportMedium TransportMedium
        {
            get { return SubscriptionTransportMedium.MongoDB; }
        }

        /// <summary>
        /// Gets whether or not there's more data to be read in the stream
        /// </summary>
        public bool EndOfStream {
            get { return enumerator == null || _endOfStream;  }
        }

        /// <summary>
        /// Gets the next line/batch of content from the stream
        /// </summary>
        public string ReadLine() {
            var current = _next;
            _endOfStream = !enumerator.MoveNext();
            _next = enumerator.Current;
            return current;
        }


        /// <summary>
        /// Direct access to the StreamReader instance
        /// </summary>
        public StreamReader StreamReader { get; private set; }

        /// <summary>
        /// Gets whether or not this stream reader should be rate limited
        /// </summary>
        public bool ShouldBeRateLimited => false;

        /// <summary>
        /// Disposes of the stream
        /// </summary>
        public void Dispose()
        {
        }
    }
}
