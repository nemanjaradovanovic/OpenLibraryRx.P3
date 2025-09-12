using System;
using System.Collections.Concurrent;
using System.Threading;

namespace OpenLibraryRx.P3
{
    public sealed class ResponseCache : IDisposable
    {
        private sealed class Entry
        {
            public byte[] Data;
            public DateTimeOffset ExpiresAt;
        }

        private readonly ConcurrentDictionary<string, Entry> _map = new ConcurrentDictionary<string, Entry>();
        private readonly TimeSpan _ttl;
        private readonly Timer _timer;
        private long _hits, _misses;

        public ResponseCache(TimeSpan ttl)
        {
            _ttl = ttl <= TimeSpan.Zero ? TimeSpan.FromMinutes(5) : ttl;
            _timer = new Timer(Cleanup, null, _ttl, _ttl);
        }

        public bool TryGet(string key, out byte[] data)
        {
            if (_map.TryGetValue(key, out var e))
            {
                if (e.ExpiresAt > DateTimeOffset.UtcNow)
                {
                    Interlocked.Increment(ref _hits);
                    data = e.Data;
                    return true;
                }
                _map.TryRemove(key, out _);
            }
            Interlocked.Increment(ref _misses);
            data = null;
            return false;
        }

        public void Set(string key, byte[] data)
        {
            _map[key] = new Entry
            {
                Data = data,
                ExpiresAt = DateTimeOffset.UtcNow + _ttl
            };
        }

        private void Cleanup(object state)
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var kv in _map)
            {
                if (kv.Value.ExpiresAt <= now)
                    _map.TryRemove(kv.Key, out _);
            }
        }

        public object Stats => new
        {
            hits = Interlocked.Read(ref _hits),
            misses = Interlocked.Read(ref _misses),
            size = _map.Count,
            ttlSeconds = (int)_ttl.TotalSeconds
        };

        public void Dispose()
        {
            _timer?.Dispose();
            _map.Clear();
        }
    }
}
