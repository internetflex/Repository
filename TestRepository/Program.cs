using System;
using System.Collections.Generic;
using Repository;

namespace TestRepository
{
    public class Program
    {
        const string IndexFile = @"D:\Projects\Repository\index.csv";
        const string DataFile = @"D:\Projects\Repository\data.json";
        const string CacheFile = @"D:\Projects\Repository\cache.json";

        static void Main(string[] args)
        {
            SampleData();

            using (var repository = Repository.Repository.Open(IndexFile, DataFile, CacheFile))
            {
                var data = repository.Fetch(Key.New(3));
                var record = (SomeData)data;

                record.Name = "Boris";
                repository.Update(Key.New(3), record);

                record.Name = "Bob";
                repository.Update(Key.New(2), record);

                record = (SomeData)repository.Fetch(Key.New(1));
                record.Name = "Kevin Bentley";
                repository.Update(Key.New(1), record);

                record.Name = "Kevin David Bentley";
                repository.Update(Key.New(1), record);

                var newRecord = new SomeData {Age = 61, Name = "Asli", DateTime = DateTime.Now, Decimal = 1.3M, List = new List<int> {7, 8, 9}};
                var key = repository.Add(newRecord);

                repository.Delete(Key.New(4));

                record = (SomeData)repository.Fetch(Key.New(1));

                repository.Compress();
            }

            using (var repository = Repository.Repository.Open(IndexFile, DataFile, CacheFile))
            {
                var data = repository.Fetch(Key.New(2));
                var record = (SomeData)data;
            }
        }

        private static void SampleData()
        {
            var data = new SomeData
            {
                DateTime = DateTime.Now,
                Name = "Kevin",
                Age = 58,
                Decimal = 23.75M,
                List = new List<int> { 5, 6, 7, 8 }
            };

            Repository.Repository.Create(IndexFile, DataFile);

            using (var repository = Repository.Repository.Open(IndexFile, DataFile, CacheFile))
            {
                var key1 = repository.Add(data);

                data.Age = 59;
                var key2 = repository.Add(data);

                data.Age = 60;
                var key3 = repository.Add(data);

                data.Age = 61;
                var key4 = repository.Add(data);

                data.Age = 62;
                var key5 = repository.Add(data);
            }
        }

        public class SomeData
        {
            public DateTime DateTime { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public decimal Decimal { get; set; }
            public List<int> List { get; set; }
        }
    }
}
