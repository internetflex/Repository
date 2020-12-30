using System.IO;
using System.Text;

namespace Repository
{
    public class MemoryFileHeader
    {
        public static uint HeaderSize = (uint)new MemoryFileHeader(0, 0, 0, 0, Key.Empty).FormatHeader().Length;

        private MemoryFileHeader(ushort indexMaxPages, ushort dataMaxPages, uint indexTail, uint dataTail, Key lastKey)
        {
            IndexMaxPages = indexMaxPages;
            DataMaxPages = dataMaxPages;
            IndexTail = indexTail;
            DataTail = dataTail;
            LastKey = lastKey;
        }

        public ushort IndexMaxPages { get; }
        public ushort DataMaxPages { get; }
        public uint IndexTail { get; private set; }
        public uint DataTail { get; private set; }
        public Key LastKey { get; private set; }

        public byte[] FormatHeader()
        {
            return Encoding.ASCII.GetBytes($"{IndexMaxPages:00000},{DataMaxPages:00000},{IndexTail:0000000000},{DataTail:0000000000},{LastKey.Value:0000000000}\r\n");
        }

        public static MemoryFileHeader Read(Stream stream)
        {
            stream.Seek(0, SeekOrigin.Begin);
            var readBuffer = new byte[HeaderSize];
            stream.Read(readBuffer, 0, (int)HeaderSize);
            var indexString = Encoding.ASCII.GetString(readBuffer);
            var parts = indexString.Split(',');
            var indexPageCount = ushort.Parse(parts[0]);
            var dataPageCount = ushort.Parse(parts[1]);
            var indexTail = uint.Parse(parts[2]);
            var dataTail = uint.Parse(parts[3]);
            var lastKey = new Key(uint.Parse(parts[4]));
            return new MemoryFileHeader(indexPageCount, dataPageCount, indexTail, dataTail, lastKey);
        }

        public void Update(Stream stream, Key newKey)
        {
            stream.Seek(0, SeekOrigin.Begin);
            LastKey = newKey;
            var buffer = FormatHeader();
            stream.Write(buffer, 0, buffer.Length);
        }

        public void Update(Stream stream, uint indexTail, uint dataTail)
        {
            stream.Seek(0, SeekOrigin.Begin);
            IndexTail = indexTail;
            DataTail = dataTail;
            var buffer = FormatHeader();
            stream.Write(buffer, 0, buffer.Length);
        }

        public void Update(Stream stream, uint indexTail, uint dataTail, Key key)
        {
            stream.Seek(0, SeekOrigin.Begin);
            IndexTail = indexTail;
            DataTail = dataTail;
            LastKey = key;
            var buffer = FormatHeader();
            stream.Write(buffer, 0, buffer.Length);
        }

        public static void Save(Stream stream, ushort indexMaxPages, ushort dataMaxPages, uint indexTail, uint dataTail, Key lastKey)
        {
            var mfh = new MemoryFileHeader(indexMaxPages, dataMaxPages, indexTail, dataTail, lastKey);
            stream.Seek(0, SeekOrigin.Begin);
            var buffer = mfh.FormatHeader();
            stream.Write(buffer, 0, buffer.Length);
        }
    }
}