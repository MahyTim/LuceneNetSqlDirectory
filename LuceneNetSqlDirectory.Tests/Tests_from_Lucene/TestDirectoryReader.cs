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


using System;
using Lucene.Net.Index;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Directory = Lucene.Net.Store.Directory;

namespace LuceneNetSqlDirectory.Tests
{
	
    [TestClass]
	public class TestDirectoryReader:LuceneTestCase
	{
		protected internal SqlServerDirectory Dir;
		private Document _doc1;
		private Document _doc2;
		protected internal SegmentReader[] Readers = new SegmentReader[2];
		protected internal SegmentInfos Sis;
		
		
		[TestInitialize]
		public override void TestInitialize()
		{
			base.TestInitialize();
			Dir = new SqlServerDirectory(Connection,new Options());
			_doc1 = new Document();
			_doc2 = new Document();
			DocHelper.SetupDoc(_doc1);
			DocHelper.SetupDoc(_doc2);
			DocHelper.WriteDoc(Dir, _doc1);
			DocHelper.WriteDoc(Dir, _doc2);
			Sis = new SegmentInfos();
			Sis.Read(Dir);
		}
		
		protected internal virtual IndexReader OpenReader()
		{
			IndexReader reader;
			reader = IndexReader.Open(Dir, false);
			Assert.IsTrue(reader is DirectoryReader);
			
			Assert.IsTrue(Dir != null);
			Assert.IsTrue(Sis != null);
			Assert.IsTrue(reader != null);
			
			return reader;
		}
		
        [TestMethod]
		public virtual void  Test()
		{
			DoTestDocument();
			DoTestUndeleteAll();
		}
		
		public virtual void  DoTestDocument()
		{
			Sis.Read(Dir);
			IndexReader reader = OpenReader();
			Assert.IsTrue(reader != null);
			Document newDoc1 = reader.Document(0);
			Assert.IsTrue(newDoc1 != null);
			Assert.IsTrue(DocHelper.NumFields(newDoc1) == DocHelper.NumFields(_doc1) - DocHelper.Unstored.Count);
			Document newDoc2 = reader.Document(1);
			Assert.IsTrue(newDoc2 != null);
			Assert.IsTrue(DocHelper.NumFields(newDoc2) == DocHelper.NumFields(_doc2) - DocHelper.Unstored.Count);
			ITermFreqVector vector = reader.GetTermFreqVector(0, DocHelper.TextField2Key);
			Assert.IsTrue(vector != null);
			TestSegmentReader.CheckNorms(reader);
		}
		
		public virtual void  DoTestUndeleteAll()
		{
			Sis.Read(Dir);
			IndexReader reader = OpenReader();
			Assert.IsTrue(reader != null);
			Assert.AreEqual(2, reader.NumDocs());
			reader.DeleteDocument(0);
			Assert.AreEqual(1, reader.NumDocs());
			reader.UndeleteAll();
			Assert.AreEqual(2, reader.NumDocs());
			
			// Ensure undeleteAll survives commit/close/reopen:
			reader.Commit();
			reader.Close();
			
			if (reader is MultiReader)
			// MultiReader does not "own" the directory so it does
			// not write the changes to sis on commit:
				Sis.Commit(Dir);
			
			Sis.Read(Dir);
			reader = OpenReader();
			Assert.AreEqual(2, reader.NumDocs());
			
			reader.DeleteDocument(0);
			Assert.AreEqual(1, reader.NumDocs());
			reader.Commit();
			reader.Close();
			if (reader is MultiReader)
			// MultiReader does not "own" the directory so it does
			// not write the changes to sis on commit:
				Sis.Commit(Dir);
			Sis.Read(Dir);
			reader = OpenReader();
			Assert.AreEqual(1, reader.NumDocs());
		}
		
		
		public virtual void  _testTermVectors()
		{
			MultiReader reader = new MultiReader(Readers);
			Assert.IsTrue(reader != null);
		}
		
		
		
        [TestMethod]
		public virtual void  TestMultiTermDocs()
		{
            SqlServerDirectory.ProvisionDatabase(Connection, "test1");
            SqlServerDirectory.ProvisionDatabase(Connection, "test2");
            SqlServerDirectory.ProvisionDatabase(Connection, "test3");

            var ramDir1 = new SqlServerDirectory(Connection, new Options() { SchemaName = "test1"});
            
			AddDoc(ramDir1, "test foo", true);
			var ramDir2 = new SqlServerDirectory(Connection, new Options() { SchemaName = "test2" });
            AddDoc(ramDir2, "test blah", true);
			var ramDir3 = new SqlServerDirectory(Connection, new Options() { SchemaName = "test3" });
            AddDoc(ramDir3, "test wow", true);

            IndexReader[] readers1 = new [] { IndexReader.Open(ramDir1, false), IndexReader.Open(ramDir3, false) };
            IndexReader[] readers2 = new [] { IndexReader.Open(ramDir1, false), IndexReader.Open(ramDir2, false), IndexReader.Open(ramDir3, false) };
			MultiReader mr2 = new MultiReader(readers1);
			MultiReader mr3 = new MultiReader(readers2);
			
			// test mixing up TermDocs and TermEnums from different readers.
			TermDocs td2 = mr2.TermDocs();
			TermEnum te3 = mr3.Terms(new Term("body", "wow"));
			td2.Seek(te3);
			int ret = 0;
			
			// This should blow up if we forget to check that the TermEnum is from the same
			// reader as the TermDocs.
			while (td2.Next())
				ret += td2.Doc;
			td2.Close();
			te3.Close();
			
			// really a dummy assert to ensure that we got some docs and to ensure that
			// nothing is optimized out.
			Assert.IsTrue(ret > 0);
		}
		
        [TestMethod]
		public virtual void  TestAllTermDocs()
		{
			IndexReader reader = OpenReader();
			int numDocs = 2;
			TermDocs td = reader.TermDocs(null);
			for (int i = 0; i < numDocs; i++)
			{
				Assert.IsTrue(td.Next());
				Assert.AreEqual(i, td.Doc);
				Assert.AreEqual(1, td.Freq);
			}
			td.Close();
			reader.Close();
		}
		
		private void  AddDoc(SqlServerDirectory ramDir1, System.String s, bool create)
		{
			IndexWriter iw = new IndexWriter(ramDir1, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_CURRENT), create, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("body", s, Field.Store.YES, Field.Index.ANALYZED));
			iw.AddDocument(doc);
			iw.Close();
		}
	}
}