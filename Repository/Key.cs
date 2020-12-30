namespace Repository
{
    public struct Key
    {
        public static Key Empty = new Key(0);

        public static Key New(uint value)
        {
            return new Key(value);
        }

        public Key(uint value)
        {
            Value = value;
        }

        public uint Value { get; }

        public static bool operator<(Key left, Key right)
        {
            return left.Value < right.Value;
        }

        public static bool operator >(Key left, Key right)
        {
            return left.Value > right.Value;
        }

        public static bool operator ==(Key left, Key right)
        {
            return left.Value == right.Value;
        }

        public static bool operator !=(Key left, Key right)
        {
            return left.Value == right.Value;
        }

        public static Key operator +(Key key, uint right)
        {
            if (uint.MaxValue - key.Value > right)
                return new Key(key.Value + right);
            throw new RepositoryException($"Cannot add number {right} to key {key.Value} as resulting key must be < {uint.MaxValue}");
        }

        public static Key operator -(Key key, uint right)
        {
            if (key.Value > right)
                return new Key(key.Value - right);
            throw new RepositoryException($"Cannot subtract number {right} from key {key.Value} as resulting key must be > 0");
        }

        public override string ToString()
        {
            return Value.ToString();
        }

        public bool Equals(Key other)
        {
            return Value == other.Value;
        }

        public override bool Equals(object obj)
        {
            return obj is Key other && Equals(other);
        }

        public override int GetHashCode()
        {
            return (int)Value;
        }
    }
}
