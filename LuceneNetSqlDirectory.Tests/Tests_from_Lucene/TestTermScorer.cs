/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


using Lucene.Net.Search;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;

namespace LuceneNetSqlDirectory.Tests
{

    [TestClass]
    public class TestTermScorer : LuceneTestCase
    {
        private class AnonymousClassCollector : Collector
        {
            public AnonymousClassCollector(System.Collections.IList docs, TestTermScorer enclosingInstance)
            {
                InitBlock(docs, enclosingInstance);
            }
            private void InitBlock(System.Collections.IList docs, TestTermScorer enclosingInstance)
            {
                this._docs = docs;
                this._enclosingInstance = enclosingInstance;
            }
            private System.Collections.IList _docs;
            private TestTermScorer _enclosingInstance;
            public TestTermScorer EnclosingInstance
            {
                get
                {
                    return _enclosingInstance;
                }

            }
            private int _baseRenamed = 0;
            private Scorer _scorer;
            public override void SetScorer(Scorer scorer)
            {
                this._scorer = scorer;
            }

            public override void Collect(int doc)
            {
                float score = _scorer.Score();
                doc = doc + _baseRenamed;
                _docs.Add(new TestHit(_enclosingInstance, doc, score));
                Assert.IsTrue(score > 0, "score " + score + " is not greater than 0");
                Assert.IsTrue(doc == 0 || doc == 5, "Doc: " + doc + " does not equal 0 or doc does not equal 5");
            }
            public override void SetNextReader(IndexReader reader, int docBase)
            {
                _baseRenamed = docBase;
            }

            public override bool AcceptsDocsOutOfOrder
            {
                get { return true; }
            }
        }
        protected internal SqlServerDirectory Directory;
        private const System.String FIELD = "field";

        protected internal System.String[] Values = new System.String[] { "all", "dogs dogs", "like", "playing", "fetch", "all" };
        protected internal IndexSearcher IndexSearcher;
        protected internal IndexReader indexReader;


        [TestInitialize]
        public override void TestInitialize()
        {
            base.TestInitialize();
            Directory = new SqlServerDirectory(Connection, new Options());


            IndexWriter writer = new IndexWriter(Directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
            for (int i = 0; i < Values.Length; i++)
            {
                Document doc = new Document();
                doc.Add(new Field(FIELD, Values[i], Field.Store.YES, Field.Index.ANALYZED));
                writer.AddDocument(doc);
            }
            writer.Close();
            IndexSearcher = new IndexSearcher(Directory, false);
            indexReader = IndexSearcher.IndexReader;
        }

        [TestMethod]
        public virtual void Test()
        {

            Term allTerm = new Term(FIELD, "all");
            TermQuery termQuery = new TermQuery(allTerm);

            Weight weight = termQuery.Weight(IndexSearcher);

            TermScorer ts = new TermScorer(weight, indexReader.TermDocs(allTerm), IndexSearcher.Similarity, indexReader.Norms(FIELD));
            //we have 2 documents with the term all in them, one document for all the other values
            System.Collections.IList docs = new System.Collections.ArrayList();
            //must call next first


            ts.Score(new AnonymousClassCollector(docs, this));
            Assert.IsTrue(docs.Count == 2, "docs Size: " + docs.Count + " is not: " + 2);
            TestHit doc0 = (TestHit)docs[0];
            TestHit doc5 = (TestHit)docs[1];
            //The scores should be the same
            Assert.IsTrue(doc0.Score == doc5.Score, doc0.Score + " does not equal: " + doc5.Score);
            /*
			Score should be (based on Default Sim.:
			All floats are approximate
			tf = 1
			numDocs = 6
			docFreq(all) = 2
			idf = ln(6/3) + 1 = 1.693147
			idf ^ 2 = 2.8667
			boost = 1
			lengthNorm = 1 //there is 1 term in every document
			coord = 1
			sumOfSquaredWeights = (idf * boost) ^ 2 = 1.693147 ^ 2 = 2.8667
			queryNorm = 1 / (sumOfSquaredWeights)^0.5 = 1 /(1.693147) = 0.590
			
			score = 1 * 2.8667 * 1 * 1 * 0.590 = 1.69
			
			*/
            Assert.IsTrue(doc0.Score == 1.6931472f, doc0.Score + " does not equal: " + 1.6931472f);
        }

        [TestMethod]
        public virtual void TestNext()
        {

            Term allTerm = new Term(FIELD, "all");
            TermQuery termQuery = new TermQuery(allTerm);

            Weight weight = termQuery.Weight(IndexSearcher);

            TermScorer ts = new TermScorer(weight, indexReader.TermDocs(allTerm), IndexSearcher.Similarity, indexReader.Norms(FIELD));
            Assert.IsTrue(ts.NextDoc() != DocIdSetIterator.NO_MORE_DOCS, "next did not return a doc");
            Assert.IsTrue(ts.Score() == 1.6931472f, "score is not correct");
            Assert.IsTrue(ts.NextDoc() != DocIdSetIterator.NO_MORE_DOCS, "next did not return a doc");
            Assert.IsTrue(ts.Score() == 1.6931472f, "score is not correct");
            Assert.IsTrue(ts.NextDoc() == DocIdSetIterator.NO_MORE_DOCS, "next returned a doc and it should not have");
        }

        [TestMethod]
        public virtual void TestSkipTo()
        {

            Term allTerm = new Term(FIELD, "all");
            TermQuery termQuery = new TermQuery(allTerm);

            Weight weight = termQuery.Weight(IndexSearcher);

            TermScorer ts = new TermScorer(weight, indexReader.TermDocs(allTerm), IndexSearcher.Similarity, indexReader.Norms(FIELD));
            Assert.IsTrue(ts.Advance(3) != DocIdSetIterator.NO_MORE_DOCS, "Didn't skip");
            //The next doc should be doc 5
            Assert.IsTrue(ts.DocID() == 5, "doc should be number 5");
        }

        private class TestHit
        {
            private void InitBlock(TestTermScorer enclosingInstance)
            {
                this._enclosingInstance = enclosingInstance;
            }
            private TestTermScorer _enclosingInstance;
            public TestTermScorer EnclosingInstance
            {
                get
                {
                    return _enclosingInstance;
                }

            }
            public int Doc;
            public float Score;

            public TestHit(TestTermScorer enclosingInstance, int doc, float score)
            {
                InitBlock(enclosingInstance);
                this.Doc = doc;
                this.Score = score;
            }

            public override System.String ToString()
            {
                return "TestHit{" + "doc=" + Doc + ", score=" + Score + "}";
            }
        }
    }
}