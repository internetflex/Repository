using System;

namespace Repository
{
    public class RepositoryException : ApplicationException
    {
        public RepositoryException(string message) :
            base(message)
        {
        }
    }
}
