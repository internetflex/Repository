using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Text.Json;

namespace Repository
{
    public class Repository : IRepository
    {
        private readonly MemoryMappedFile _indexMappedFile;
        private readonly MemoryMappedFile _dataMappedFile;
        private readonly string _indexFile;
        private readonly string _dataFile;
        private readonly string _cacheFile;
        private readonly KeyLookupCache _keyLookupCache;

        private class DataWrapper
        {
            public string Typename { get; set; }
            public object Record { get; set; }
        }

        private Repository(string indexFile, string dataFile, string cacheFile)
        {
            _indexFile = indexFile;
            _dataFile = dataFile;
            _cacheFile = cacheFile;
            _keyLookupCache = new KeyLookupCache();

            MemoryFileHeader header;

            using (var fileStream = File.Open(indexFile, FileMode.Open))
            {
                header = MemoryFileHeader.Read(fileStream);
            }

            _indexMappedFile = MemoryMappedFile.CreateFromFile(indexFile, FileMode.Open, null, 4096 * header.IndexMaxPages);
            _dataMappedFile = MemoryMappedFile.CreateFromFile(dataFile, FileMode.Open, null, 4096 * header.DataMaxPages);
//            if (File.Exists(_cacheFile))
//                _keyLookupCache = KeyLookupCache.Load(_cacheFile);
        }

        public static IRepository Open(string indexFile, string dataFile, string cacheFile)
        {
            return new Repository(indexFile, dataFile, cacheFile);
        }

        public static void Create(string indexFile, string dataFile, ushort dataPageCount = 256)
        {
            var indexPageCount = (ushort)Math.Ceiling((decimal)(dataPageCount / 64));
            var indexMappedFile = MemoryMappedFile.CreateFromFile(indexFile, FileMode.Create, null, 4096 * indexPageCount);
            using (var indexStream = indexMappedFile.CreateViewStream())
            {
                MemoryFileHeader.Save(indexStream, indexPageCount, dataPageCount, MemoryFileHeader.HeaderSize, 0, Key.Empty);
            }
            indexMappedFile.Dispose();

            var dataMappedFile = MemoryMappedFile.CreateFromFile(dataFile, FileMode.Create, null, 4096 * dataPageCount);
            dataMappedFile.Dispose();
        }

        public object Fetch(Key key)
        {
            var offset = FindOffset(key);
            return offset != 0 ? Read(offset) : null;
        }

        public void Update(Key key, object dataRecord)
        {
            var indexOffset = FindOffset(key);

            using (var indexStream = _indexMappedFile.CreateViewStream())
            using (var dataStream = _dataMappedFile.CreateViewStream())
            {
                var mfh = MemoryFileHeader.Read(indexStream);
                indexStream.Seek(indexOffset, SeekOrigin.Begin);
                var mfi = FindNonLinkedRecord(indexStream);

                if (mfi.State == RecordState.Deleted)
                    throw new RepositoryException($"Cannot update deleted record for key '{mfi.Key}'");

                if (mfi.Typename.Trim() != dataRecord.GetType().Name)
                    throw new RepositoryException($"Update type '{dataRecord.GetType().Name}' not same as stored type '{mfi.Typename}'");

                var wrappedRecord = new DataWrapper { Typename = dataRecord.GetType().AssemblyQualifiedName, Record = dataRecord};
                var jsonString = JsonSerializer.Serialize(wrappedRecord);
                indexStream.Seek(-MemoryFileIndex.IndexLength, SeekOrigin.Current); // rewind back to beginning of record to overwrite

                if (jsonString.Length + 2 > mfi.Length)
                {
                    mfi.Update(indexStream, mfh.IndexTail, mfi.State == RecordState.Ok ? RecordState.Head : RecordState.Linked);
                    Add(indexStream, dataStream, mfi.Key, dataRecord, RecordState.Tail);
                }
                else
                {
                    jsonString = jsonString.PadRight((int)mfi.Length - 2, ' ') + "\r\n";
                    var jsonBuffer = Encoding.ASCII.GetBytes(jsonString);
                    dataStream.Seek(mfi.Offset, SeekOrigin.Begin);
                    dataStream.Write(jsonBuffer, 0, jsonBuffer.Length);
                }
            }
        }

        public Key Add(object data)
        {
            using (var indexStream = _indexMappedFile.CreateViewStream())
            using (var dataStream = _dataMappedFile.CreateViewStream())
            {
                var newKey = GetNextKey(indexStream);
                Add(indexStream, dataStream, newKey, data);
                return newKey;
            }
        }

        public void Delete(Key key)
        {
            var indexOffset = FindOffset(key);
            var indexSize = MemoryFileIndex.IndexLength;

            using (var indexStream = _indexMappedFile.CreateViewStream())
            {
                indexStream.Seek(indexOffset, SeekOrigin.Begin);
                var mfi = MemoryFileIndex.Load(indexStream);
                indexStream.Seek(-indexSize, SeekOrigin.Current); // rewind back to beginning of record to overwrite

                if (mfi.State != RecordState.Deleted)
                {
                    var deletedMfi = new MemoryFileIndex(mfi.Offset, mfi.Length, mfi.Key, mfi.Typename, RecordState.Deleted);
                    var indexBuffer = Encoding.ASCII.GetBytes(deletedMfi.GetIndex());
                    indexStream.Write(indexBuffer, 0, indexBuffer.Length);
                }
            }
        }

        private Key GetNextKey(Stream indexStream)
        {
            var mfh = MemoryFileHeader.Read(indexStream);
            var newKey = mfh.LastKey + 1;
            mfh.Update(indexStream, newKey);
            return newKey;
        }

        private void Add(Stream indexStream, Stream dataStream, Key key, object data, RecordState newState = RecordState.Ok)
        {
            var indexSize = MemoryFileIndex.IndexLength;
            var mfh = MemoryFileHeader.Read(indexStream);
            var wrappedObject = new DataWrapper {Typename = data.GetType().AssemblyQualifiedName, Record = data};
            var jsonString = JsonSerializer.Serialize(wrappedObject) + "\r\n";
            dataStream.Seek(mfh.DataTail, SeekOrigin.Begin);
            var jsonBuffer = Encoding.ASCII.GetBytes(jsonString);
            dataStream.Write(jsonBuffer, 0, jsonBuffer.Length);
            _keyLookupCache.Update(key, mfh.IndexTail);

            var mfi = new MemoryFileIndex(mfh.DataTail, (uint)jsonBuffer.Length, key, data.GetType().Name, newState);
            indexStream.Seek(mfh.IndexTail, SeekOrigin.Begin);
            var indexBuffer = Encoding.ASCII.GetBytes(mfi.GetIndex());
            indexStream.Write(indexBuffer, 0, indexBuffer.Length);

            mfh.Update(indexStream, mfh.IndexTail + indexSize, (uint)(mfh.DataTail + jsonBuffer.Length));
        }

        private void Copy(string jsonData, string typename, Key currentKey)
        {
            using (var indexStream = _indexMappedFile.CreateViewStream())
            using (var dataStream = _dataMappedFile.CreateViewStream())
            {
                Copy(indexStream, dataStream, jsonData, typename, currentKey);
            }
        }

        private static void Copy(Stream indexStream, Stream dataStream, string json, string typename, Key currentKey)
        {
            var indexSize = MemoryFileIndex.IndexLength;
            var mfh = MemoryFileHeader.Read(indexStream);
            if (currentKey < mfh.LastKey)
                currentKey = mfh.LastKey;

            dataStream.Seek(mfh.DataTail, SeekOrigin.Begin);
            var jsonBuffer = Encoding.ASCII.GetBytes(json);
            dataStream.Write(jsonBuffer, 0, jsonBuffer.Length);

            var mfi = new MemoryFileIndex(mfh.DataTail, (uint)jsonBuffer.Length, currentKey, typename, RecordState.Ok);
            indexStream.Seek(mfh.IndexTail, SeekOrigin.Begin);
            var indexBuffer = Encoding.ASCII.GetBytes(mfi.GetIndex());
            indexStream.Write(indexBuffer, 0, indexBuffer.Length);

            mfh.Update(indexStream, mfh.IndexTail + indexSize, (uint)(mfh.DataTail + jsonBuffer.Length), currentKey);
        }

        private object Read(uint offset)
        {
            object record = null;

            using (var indexStream = _indexMappedFile.CreateViewStream())
            using (var dataStream = _dataMappedFile.CreateViewStream())
            {
                indexStream.Seek(offset, SeekOrigin.Begin);
                var mfi = FindNonLinkedRecord(indexStream);

                if (mfi.State == RecordState.Ok || mfi.State == RecordState.Tail)
                {
                    dataStream.Seek(mfi.Offset, SeekOrigin.Begin);
                    var inputBuffer = new byte[mfi.Length];
                    dataStream.Read(inputBuffer, 0, (int)mfi.Length);
                    var jsonData = Encoding.ASCII.GetString(inputBuffer);
                    var wrappedType = (DataWrapper)JsonSerializer.Deserialize(jsonData, typeof(DataWrapper));
                    var element = (JsonElement) wrappedType.Record;
                    var userType = Type.GetType(wrappedType.Typename, true);
                    record = JsonSerializer.Deserialize(element.GetRawText(), userType);
                }
            }

            return record;
        }

        public void Compress()
        {
            var indexFile = Path.GetTempFileName();
            var dataFile = Path.GetTempFileName();
            var currentOffset = MemoryFileHeader.HeaderSize;
            var indexSize = MemoryFileIndex.IndexLength;

            using (var indexStream = _indexMappedFile.CreateViewStream())
            using (var dataStream = _dataMappedFile.CreateViewStream())
            {
                var header = MemoryFileHeader.Read(indexStream);
                Create(indexFile, dataFile, header.DataMaxPages);

                using (var newRepository = Open(indexFile, dataFile, _cacheFile))
                {
                    while (currentOffset < header.IndexTail)
                    {
                        indexStream.Seek(currentOffset, SeekOrigin.Begin);
                        var mfi = FindNonLinkedRecord(indexStream);

                        if (mfi.State == RecordState.Ok || mfi.State == RecordState.Tail)
                        {
                            dataStream.Seek(mfi.Offset, SeekOrigin.Begin);
                            var inputBuffer = new byte[mfi.Length];
                            dataStream.Read(inputBuffer, 0, (int) mfi.Length);
                            var jsonData = Encoding.ASCII.GetString(inputBuffer);
                            jsonData = jsonData.Substring(0, jsonData.Length - 2).TrimEnd() + "\r\n";
                            ((Repository)newRepository).Copy(jsonData, mfi.Typename, mfi.Key);
                            indexStream.Seek(-MemoryFileIndex.IndexLength, SeekOrigin.Current);
                            mfi.Update(indexStream, RecordState.Copied);
                        }

                        currentOffset += indexSize;
                    }
                }
            }

            Dispose();

            File.Delete(_indexFile);
            File.Delete(_dataFile);
            File.Move(indexFile, _indexFile);
            File.Move(dataFile, _dataFile);
        }

        private static MemoryFileIndex FindNonLinkedRecord(Stream indexStream)
        {
            var mfi = MemoryFileIndex.Load(indexStream);

            while (mfi.State == RecordState.Head || mfi.State == RecordState.Linked)
            {
                indexStream.Seek(mfi.Offset, SeekOrigin.Begin);
                mfi = MemoryFileIndex.Load(indexStream);
            }

            return mfi;
        }

        private uint FindOffset(Key key)
        {
            var offset = _keyLookupCache.Get(key);
            if (offset > 0)
                return offset;

            var indexSize = MemoryFileIndex.IndexLength;

            using (var indexStream = _indexMappedFile.CreateViewStream())
            {
                const uint lowerIndex = 1U;
                var header = MemoryFileHeader.Read(indexStream);
                var upperIndex = (header.IndexTail - MemoryFileHeader.HeaderSize) / indexSize;
                offset = BinarySearch(indexStream, lowerIndex, upperIndex);
                if (offset != 0)
                    _keyLookupCache.Update(key, offset);
                return offset;
            }

            uint BinarySearch(Stream stream, uint lower, uint upper)
            {
                while (true)
                {
                    if (lower > upper) return 0;

                    var middleIndex = (lower + upper) / 2;
                    var currentOffset = (middleIndex - 1) * indexSize + MemoryFileHeader.HeaderSize;
                    stream.Seek(currentOffset, SeekOrigin.Begin);
                    var mfi = MemoryFileIndex.Load(stream);

                    if (mfi.State == RecordState.Ok || mfi.State == RecordState.Head)
                    {
                        if (mfi.Key == key) return currentOffset;

                        if (lower == upper) return 0;

                        if (key < mfi.Key)
                        {
                            lower = 1;
                            upper = middleIndex - 1;
                            continue;
                        }

                        lower = middleIndex + 1;
                        continue;
                    }

                    var result = BinarySearch(stream, lower, middleIndex);
                    if (result != 0) return result;
                    lower = middleIndex;
                }
            }
        }

        private void Truncate()
        {
            using (var fileStream = File.Open(_indexFile, FileMode.Open))
            {
                var header = MemoryFileHeader.Read(fileStream);
                fileStream.SetLength(header.IndexTail);

                using (var dataStream = File.Open(_dataFile, FileMode.Open))
                {
                    dataStream.SetLength(header.DataTail);
                }
            }
        }

        public void Dispose()
        {
            _indexMappedFile.Dispose();
            _dataMappedFile.Dispose();
            _keyLookupCache.Save(_cacheFile);
            Truncate();
        }
    }
}
