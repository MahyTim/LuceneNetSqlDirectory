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

using Lucene.Net.Documents;
using Lucene.Net.Index;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Document = Lucene.Net.Documents.Document;
using DefaultSimilarity = Lucene.Net.Search.DefaultSimilarity;

namespace LuceneNetSqlDirectory.Tests
{
	
    [TestClass]
	public class TestSegmentReader:LuceneTestCase
    {
        private SqlServerDirectory _dir;
		private Document _testDoc = new Document();
		private SegmentReader _reader = null;
		
		
		//TODO: Setup the reader w/ multiple documents
		[TestInitialize]
		public override void TestInitialize()
		{
			base.TestInitialize();
            _testDoc = new Document();

            _dir = new SqlServerDirectory(Connection,new Options());
			DocHelper.SetupDoc(_testDoc);
			SegmentInfo info = DocHelper.WriteDoc(_dir, _testDoc);
            _reader = SegmentReader.Get(true, info, 1);
		}

		[TestMethod]
		public virtual void  Test()
		{
			Assert.IsTrue(_dir != null);
			Assert.IsTrue(_reader != null);
			Assert.IsTrue(DocHelper.NameValues.Count > 0);
			Assert.IsTrue(DocHelper.NumFields(_testDoc) == DocHelper.All.Count);
		}
		
		[TestMethod]
		public virtual void  TestDocument()
		{
			Assert.IsTrue(_reader.NumDocs() == 1);
			Assert.IsTrue(_reader.MaxDoc >= 1);
			Document result = _reader.Document(0);
			Assert.IsTrue(result != null);
			//There are 2 unstored fields on the document that are not preserved across writing
			Assert.IsTrue(DocHelper.NumFields(result) == DocHelper.NumFields(_testDoc) - DocHelper.Unstored.Count);
			
			var fields = result.GetFields();
            foreach (var field in fields)
			{
				Assert.IsTrue(field != null);
				Assert.IsTrue(DocHelper.NameValues.Contains(field.Name));
			}
		}
		
		[TestMethod]
		public virtual void  TestDelete()
		{
			Document docToDelete = new Document();
			DocHelper.SetupDoc(docToDelete);
			SegmentInfo info = DocHelper.WriteDoc(_dir, docToDelete);
            SegmentReader deleteReader = SegmentReader.Get(false, info, 1);
			Assert.IsTrue(deleteReader != null);
			Assert.IsTrue(deleteReader.NumDocs() == 1);
			deleteReader.DeleteDocument(0);
			Assert.IsTrue(deleteReader.IsDeleted(0) == true);
			Assert.IsTrue(deleteReader.HasDeletions == true);
			Assert.IsTrue(deleteReader.NumDocs() == 0);
		}
		
		[TestMethod]
		public virtual void  TestGetFieldNameVariations()
		{
			System.Collections.Generic.ICollection<string> result = _reader.GetFieldNames(IndexReader.FieldOption.ALL);
			Assert.IsTrue(result != null);
			Assert.IsTrue(result.Count == DocHelper.All.Count);
			for (System.Collections.IEnumerator iter = result.GetEnumerator(); iter.MoveNext(); )
			{
				System.String s = (System.String) iter.Current;
				//System.out.println("Name: " + s);
				Assert.IsTrue(DocHelper.NameValues.Contains(s) == true || s.Equals(""));
			}
			result = _reader.GetFieldNames(IndexReader.FieldOption.INDEXED);
			Assert.IsTrue(result != null);
			Assert.IsTrue(result.Count == DocHelper.Indexed.Count);
			for (System.Collections.IEnumerator iter = result.GetEnumerator(); iter.MoveNext(); )
			{
				System.String s = (System.String) iter.Current;
				Assert.IsTrue(DocHelper.Indexed.Contains(s) == true || s.Equals(""));
			}
			
			result = _reader.GetFieldNames(IndexReader.FieldOption.UNINDEXED);
			Assert.IsTrue(result != null);
			Assert.IsTrue(result.Count == DocHelper.Unindexed.Count);
			//Get all indexed fields that are storing term vectors
			result = _reader.GetFieldNames(IndexReader.FieldOption.INDEXED_WITH_TERMVECTOR);
			Assert.IsTrue(result != null);
			Assert.IsTrue(result.Count == DocHelper.Termvector.Count);
			
			result = _reader.GetFieldNames(IndexReader.FieldOption.INDEXED_NO_TERMVECTOR);
			Assert.IsTrue(result != null);
			Assert.IsTrue(result.Count == DocHelper.Notermvector.Count);
		}
		
		[TestMethod]
		public virtual void  TestTerms()
		{
			TermEnum terms = _reader.Terms();
			Assert.IsTrue(terms != null);
			while (terms.Next() == true)
			{
				Term term = terms.Term;
				Assert.IsTrue(term != null);
				//System.out.println("Term: " + term);
				System.String fieldValue = (System.String) DocHelper.NameValues[term.Field];
				Assert.IsTrue(fieldValue.IndexOf(term.Text) != - 1);
			}
			
			TermDocs termDocs = _reader.TermDocs();
			Assert.IsTrue(termDocs != null);
			termDocs.Seek(new Term(DocHelper.TextField1Key, "field"));
			Assert.IsTrue(termDocs.Next() == true);
			
			termDocs.Seek(new Term(DocHelper.NoNormsKey, DocHelper.NoNormsText));
			Assert.IsTrue(termDocs.Next() == true);
			
			
			TermPositions positions = _reader.TermPositions();
			positions.Seek(new Term(DocHelper.TextField1Key, "field"));
			Assert.IsTrue(positions != null);
			Assert.IsTrue(positions.Doc == 0);
			Assert.IsTrue(positions.NextPosition() >= 0);
		}
		
		public static void  CheckNorms(IndexReader reader)
		{
			// test omit norms
			for (int i = 0; i < DocHelper.Fields.Length; i++)
			{
				IFieldable f = DocHelper.Fields[i];
				if (f.IsIndexed)
				{
					Assert.AreEqual(reader.HasNorms(f.Name), !f.OmitNorms);
					Assert.AreEqual(reader.HasNorms(f.Name), !DocHelper.NoNorms.Contains(f.Name));
					if (!reader.HasNorms(f.Name))
					{
						// test for fake norms of 1.0 or null depending on the flag
						byte[] norms = reader.Norms(f.Name);
						byte norm1 = DefaultSimilarity.EncodeNorm(1.0f);
						Assert.IsNull(norms);
						norms = new byte[reader.MaxDoc];
						reader.Norms(f.Name, norms, 0);
						for (int j = 0; j < reader.MaxDoc; j++)
						{
							Assert.AreEqual(norms[j], norm1);
						}
					}
				}
			}
		}
		
		[TestMethod]
		public virtual void  TestTermVectors()
		{
			ITermFreqVector result = _reader.GetTermFreqVector(0, DocHelper.TextField2Key);
			Assert.IsTrue(result != null);
			System.String[] terms = result.GetTerms();
			int[] freqs = result.GetTermFrequencies();
			Assert.IsTrue(terms != null && terms.Length == 3 && freqs != null && freqs.Length == 3);
			for (int i = 0; i < terms.Length; i++)
			{
				System.String term = terms[i];
				int freq = freqs[i];
				Assert.IsTrue(DocHelper.Field2Text.IndexOf(term) != - 1);
				Assert.IsTrue(freq > 0);
			}
			
			ITermFreqVector[] results = _reader.GetTermFreqVectors(0);
			Assert.IsTrue(results != null);
			Assert.IsTrue(results.Length == 3, "We do not have 3 term freq vectors, we have: " + results.Length);
		}
	}
}