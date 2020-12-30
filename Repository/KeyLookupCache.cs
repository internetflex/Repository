using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace Repository
{
    internal class KeyLookupCache
    {
        public KeyLookupCache(int windowSize = 128)
        {
            WindowSize = windowSize;
            CacheLookup = new Dictionary<Key, uint>(windowSize);
            CacheWindow = new Queue<Key>(windowSize);
        }

        public int WindowSize { get; set; }

        public Queue<Key> CacheWindow { get; set; }

        public Dictionary<Key, uint> CacheLookup { get; set; }

        public void Update(Key key, uint offset)
        {
            if (CacheLookup.ContainsKey(key))
            {
                CacheLookup[key] = offset;
                return;
            }

            if (CacheWindow.Count == WindowSize)
            {
                var expiredKey = CacheWindow.Dequeue();
                CacheLookup.Remove(expiredKey);
            }

            CacheLookup.Add(key, offset);
            CacheWindow.Enqueue(key);
        }

        public uint Get(Key key)
        {
            return 0;
            return CacheLookup.ContainsKey(key) ? CacheLookup[key] : 0;
        }

        public void Save(string cacheFile)
        {
            var options = new JsonSerializerOptions { WriteIndented = true, Converters = { new KeyLookupCacheConverterFactory()}};
            var cacheJson = JsonSerializer.Serialize(this, options);
            File.WriteAllText(cacheFile, cacheJson);
        }
        public static KeyLookupCache Load(string cacheFile)
        {
            var options = new JsonSerializerOptions { Converters = { new KeyLookupCacheConverterFactory()}};
            var cacheJson = File.ReadAllText(cacheFile);
            return JsonSerializer.Deserialize<KeyLookupCache>(cacheJson, options);
        }
    }
}