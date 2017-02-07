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
using Analyzer = Lucene.Net.Analysis.Analyzer;
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Directory = Lucene.Net.Store.Directory;
using Similarity = Lucene.Net.Search.Similarity;

namespace LuceneNetSqlDirectory.Tests
{
	
	class DocHelper
	{
		public const System.String Field1Text = "field one text";
		public const System.String TextField1Key = "textField1";
		public static Field TextField1;
		
		public const System.String Field2Text = "field field field two text";
		//Fields will be lexicographically sorted.  So, the order is: field, text, two
		public static readonly int[] Field2Freqs = new int[]{3, 1, 1};
		public const System.String TextField2Key = "textField2";
		public static Field TextField2;
		
		
		public const System.String Field3Text = "aaaNoNorms aaaNoNorms bbbNoNorms";
		public const System.String TextField3Key = "textField3";
		public static Field TextField3;
		
		public const System.String KeywordText = "Keyword";
		public const System.String KeywordFieldKey = "keyField";
		public static Field KeyField;
		
		public const System.String NoNormsText = "omitNormsText";
		public const System.String NoNormsKey = "omitNorms";
		public static Field NoNormsField;
		
		public const System.String NoTfText = "analyzed with no tf and positions";
		public const System.String NoTfKey = "omitTermFreqAndPositions";
		public static Field NoTfField;
		
		public const System.String UnindexedFieldText = "unindexed field text";
		public const System.String UnindexedFieldKey = "unIndField";
		public static Field UnIndField;
		
		
		public const System.String Unstored1FieldText = "unstored field text";
		public const System.String UnstoredField1Key = "unStoredField1";
		public static Field UnStoredField1;
		
		public const System.String Unstored2FieldText = "unstored field text";
		public const System.String UnstoredField2Key = "unStoredField2";
		public static Field UnStoredField2;
		
		public const System.String LazyFieldBinaryKey = "lazyFieldBinary";
		public static byte[] LazyFieldBinaryBytes;
		public static Field LazyFieldBinary;
		
		public const System.String LazyFieldKey = "lazyField";
		public const System.String LazyFieldText = "These are some field bytes";
		public static Field LazyField;
		
		public const System.String LargeLazyFieldKey = "largeLazyField";
		public static System.String LargeLazyFieldText;
		public static Field LargeLazyField;
		
		//From Issue 509
		public const System.String FieldUtf1Text = "field one \u4e00text";
		public const System.String TextFieldUtf1Key = "textField1Utf8";
		public static Field TextUtfField1;
		
		public const System.String FieldUtf2Text = "field field field \u4e00two text";
		//Fields will be lexicographically sorted.  So, the order is: field, text, two
		public static readonly int[] FieldUtf2Freqs = new int[]{3, 1, 1};
		public const System.String TextFieldUtf2Key = "textField2Utf8";
		public static Field TextUtfField2;
		
		
		
		
		public static System.Collections.IDictionary NameValues = null;
		
		// ordered list of all the fields...
		// could use LinkedHashMap for this purpose if Java1.4 is OK
        public static Field[] Fields = null;
		
		// Map<String fieldName, Fieldable field>
		public static System.Collections.IDictionary All = new System.Collections.Hashtable();
		public static System.Collections.IDictionary Indexed = new System.Collections.Hashtable();
		public static System.Collections.IDictionary Stored = new System.Collections.Hashtable();
		public static System.Collections.IDictionary Unstored = new System.Collections.Hashtable();
		public static System.Collections.IDictionary Unindexed = new System.Collections.Hashtable();
		public static System.Collections.IDictionary Termvector = new System.Collections.Hashtable();
		public static System.Collections.IDictionary Notermvector = new System.Collections.Hashtable();
		public static System.Collections.IDictionary Lazy = new System.Collections.Hashtable();
		public static System.Collections.IDictionary NoNorms = new System.Collections.Hashtable();
		public static System.Collections.IDictionary NoTf = new System.Collections.Hashtable();
		
		
		private static void  Add(System.Collections.IDictionary map, IFieldable field)
		{
			map[field.Name] = field;
		}
		
		/// <summary> Adds the fields above to a document </summary>
		/// <param name="doc">The document to write
		/// </param>
		public static void  SetupDoc(Document doc)
		{
			for (int i = 0; i < Fields.Length; i++)
			{
				doc.Add(Fields[i]);
			}
		}
		
		/// <summary> Writes the document to the directory using a segment
		/// named "test"; returns the SegmentInfo describing the new
		/// segment 
		/// </summary>
		/// <param name="dir">
		/// </param>
		/// <param name="doc">
		/// </param>
		/// <throws>  IOException </throws>
		public static SegmentInfo WriteDoc(Directory dir, Document doc)
		{
			return WriteDoc(dir, new WhitespaceAnalyzer(), Similarity.Default, doc);
		}
		
		/// <summary> Writes the document to the directory using the analyzer
		/// and the similarity score; returns the SegmentInfo
		/// describing the new segment
		/// </summary>
		/// <param name="dir">
		/// </param>
		/// <param name="analyzer">
		/// </param>
		/// <param name="similarity">
		/// </param>
		/// <param name="doc">
		/// </param>
		/// <throws>  IOException </throws>
		public static SegmentInfo WriteDoc(Directory dir, Analyzer analyzer, Similarity similarity, Document doc)
		{
			IndexWriter writer = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetSimilarity(similarity);
			//writer.setUseCompoundFile(false);
			writer.AddDocument(doc);
            writer.Commit();
			SegmentInfo info = writer.NewestSegment();
			writer.Close();
			return info;
		}
		
		public static int NumFields(Document doc)
		{
			return doc.GetFields().Count;
		}
		static DocHelper()
		{
			TextField1 = new Field(TextField1Key, Field1Text, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO);
			TextField2 = new Field(TextField2Key, Field2Text, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			TextField3 = new Field(TextField3Key, Field3Text, Field.Store.YES, Field.Index.ANALYZED);
			{
				TextField3.OmitNorms = true;
			}
			KeyField = new Field(KeywordFieldKey, KeywordText, Field.Store.YES, Field.Index.NOT_ANALYZED);
			NoNormsField = new Field(NoNormsKey, NoNormsText, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
			NoTfField = new Field(NoTfKey, NoTfText, Field.Store.YES, Field.Index.ANALYZED);
			{
				NoTfField.OmitTermFreqAndPositions = true;
			}
			UnIndField = new Field(UnindexedFieldKey, UnindexedFieldText, Field.Store.YES, Field.Index.NO);
			UnStoredField1 = new Field(UnstoredField1Key, Unstored1FieldText, Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO);
			UnStoredField2 = new Field(UnstoredField2Key, Unstored2FieldText, Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES);
			LazyField = new Field(LazyFieldKey, LazyFieldText, Field.Store.YES, Field.Index.ANALYZED);
			TextUtfField1 = new Field(TextFieldUtf1Key, FieldUtf1Text, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO);
			TextUtfField2 = new Field(TextFieldUtf2Key, FieldUtf2Text, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
            Fields = new Field[] { TextField1, TextField2, TextField3, KeyField, NoNormsField, NoTfField, UnIndField, UnStoredField1, UnStoredField2, TextUtfField1, TextUtfField2, LazyField, LazyFieldBinary, LargeLazyField };
			{
				//Initialize the large Lazy Field
				System.Text.StringBuilder buffer = new System.Text.StringBuilder();
				for (int i = 0; i < 10000; i++)
				{
					buffer.Append("Lazily loading lengths of language in lieu of laughing ");
				}
				
				try
				{
					LazyFieldBinaryBytes = System.Text.Encoding.UTF8.GetBytes("These are some binary field bytes");
				}
				catch (System.IO.IOException)
				{
				}
				LazyFieldBinary = new Field(LazyFieldBinaryKey, LazyFieldBinaryBytes, Field.Store.YES);
				Fields[Fields.Length - 2] = LazyFieldBinary;
				LargeLazyFieldText = buffer.ToString();
				LargeLazyField = new Field(LargeLazyFieldKey, LargeLazyFieldText, Field.Store.YES, Field.Index.ANALYZED);

				Fields[Fields.Length - 1] = LargeLazyField;
				for (int i = 0; i < Fields.Length; i++)
				{
					IFieldable f = Fields[i];
					Add(All, f);
					if (f.IsIndexed)
						Add(Indexed, f);
					else
						Add(Unindexed, f);
					if (f.IsTermVectorStored)
						Add(Termvector, f);
					if (f.IsIndexed && !f.IsTermVectorStored)
						Add(Notermvector, f);
					if (f.IsStored)
						Add(Stored, f);
					else
						Add(Unstored, f);
					if (f.OmitNorms)
						Add(NoNorms, f);
					if (f.OmitTermFreqAndPositions)
						Add(NoTf, f);
					if (f.IsLazy)
						Add(Lazy, f);
				}
			}
			{
				NameValues = new System.Collections.Hashtable();
				NameValues[TextField1Key] = Field1Text;
				NameValues[TextField2Key] = Field2Text;
				NameValues[TextField3Key] = Field3Text;
				NameValues[KeywordFieldKey] = KeywordText;
				NameValues[NoNormsKey] = NoNormsText;
				NameValues[NoTfKey] = NoTfText;
				NameValues[UnindexedFieldKey] = UnindexedFieldText;
				NameValues[UnstoredField1Key] = Unstored1FieldText;
				NameValues[UnstoredField2Key] = Unstored2FieldText;
				NameValues[LazyFieldKey] = LazyFieldText;
				NameValues[LazyFieldBinaryKey] = LazyFieldBinaryBytes;
				NameValues[LargeLazyFieldKey] = LargeLazyFieldText;
				NameValues[TextFieldUtf1Key] = FieldUtf1Text;
				NameValues[TextFieldUtf2Key] = FieldUtf2Text;
			}
		}
	}
}