using System;
using System.IO;
using System.Text;

namespace Repository
{
    public class MemoryFileIndex
    {
        public static uint IndexLength = (uint) new MemoryFileIndex(0,0, new Key(0), "", RecordState.Ok).GetIndex().Length;

        public MemoryFileIndex(uint offset, uint length, Key key, string typeName, RecordState state)
        {
            State = state;
            Offset = offset;
            Key = key;
            Length = length;
            Typename = typeName.PadRight(30, ' ').Substring(0, 30);
        }

        public RecordState State { get; private set; }
        public uint Offset { get; private set; }
        public uint Length { get; }
        public Key Key { get; }
        public string Typename { get; }

        public string GetIndex()
        {
            return $"{(byte)State:0},{Offset:0000000000},{Length:0000000000},{Key:0000000000},{Typename}\r\n";
        }

        public void Update(Stream stream, uint dataOffset, RecordState state)
        {
            Offset = dataOffset;
            State = state;
            var indexBuffer = Encoding.ASCII.GetBytes(GetIndex());
            stream.Write(indexBuffer, 0, indexBuffer.Length);
        }

        public void Update(Stream stream, RecordState state)
        {
            State = state;
            var indexBuffer = Encoding.ASCII.GetBytes(GetIndex());
            stream.Write(indexBuffer, 0, indexBuffer.Length);
        }

        public static MemoryFileIndex Load(Stream stream)
        {
            var readBuffer = new byte[IndexLength];
            stream.Read(readBuffer, 0, (int)IndexLength);
            var indexString = Encoding.ASCII.GetString(readBuffer);
            var parts = indexString.Split(',');
            var stateValue = byte.Parse(parts[0]);
            var state = GetState(stateValue);
            var offset = uint.Parse(parts[1]);
            var length = uint.Parse(parts[2]);
            var key = new Key(uint.Parse(parts[3]));
            var typeName = parts[4].Substring(0, parts[4].Length-2);
            return new MemoryFileIndex(offset, length, key, typeName, state);
        }

        private static RecordState GetState(byte stateValue)
        {
            return (RecordState)(stateValue);
        }
    }
}