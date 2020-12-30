namespace Repository
{
    public enum RecordState : byte
    {
        Ok = 0,
        Head = 1,
        Linked = 2,
        Tail = 3,
        Deleted = 4,
        Copied = 5
    }
}