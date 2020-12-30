using System;

namespace Repository
{
    public interface IRepository : IDisposable
    {
        object Fetch(Key key);
        void Update(Key key, object dataRecord);
        Key Add(object data);
        void Delete(Key key);
        void Compress();
    }
}