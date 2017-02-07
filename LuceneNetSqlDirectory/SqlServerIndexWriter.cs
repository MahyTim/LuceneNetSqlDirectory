using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Store;

namespace LuceneNetSqlDirectory
{
    public class SqlServerIndexWriter : IndexWriter
    {
        public SqlServerIndexWriter(SqlServerDirectory d, Analyzer a, bool create, MaxFieldLength mfl) : base(d, a, create, mfl)
        {
        }

        public SqlServerIndexWriter(SqlServerDirectory d, Analyzer a, MaxFieldLength mfl) : base(d, a, mfl)
        {
        }

        public SqlServerIndexWriter(SqlServerDirectory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl) : base(d, a, deletionPolicy, mfl)
        {
        }

        public SqlServerIndexWriter(SqlServerDirectory d, Analyzer a, bool create, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl) : base(d, a, create, deletionPolicy, mfl)
        {
        }

        public SqlServerIndexWriter(SqlServerDirectory d, Analyzer a, IndexDeletionPolicy deletionPolicy, MaxFieldLength mfl, IndexCommit commit) : base(d, a, deletionPolicy, mfl, commit)
        {
        }

        public override void Optimize()
        {
            base.Optimize();
            (base.Directory as SqlServerDirectory)?.DeleteTemporaryFiles();
        }

        public override void Optimize(bool doWait)
        {
            base.Optimize(doWait);
            (base.Directory as SqlServerDirectory)?.DeleteTemporaryFiles();
        }

        public override void Optimize(int maxNumSegments, bool doWait)
        {
            base.Optimize(maxNumSegments, doWait);
            (base.Directory as SqlServerDirectory)?.DeleteTemporaryFiles();
        }
    }
}