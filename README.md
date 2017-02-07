# LuceneNetSqlDirectory
Store your Lucene.NET files in a SQLServer using this Directory implementation.

## Why ##
[Lucene.NET](https://lucenenet.apache.org/) is a very powerful [full text search](https://en.wikipedia.org/wiki/Full_text_search) solution with advanced '[faceted search](https://en.wikipedia.org/wiki/Faceted_search)' options. Lucene.NET stores it's metadata and inverted index using 'files', and the default implementations shipped with the framework are all file system based. However this has some major drawbacks if you want to use the solution in a clustered webfarm environment and encounter a more traditional operations division. From our experience most large scale non-devops oranizations using the MS stack tend to have an operations division that does not like shared file systems and cannot come up with decent backup strategies that are aligned with database backups.

Being fed up of having to go into discussions on shared filesystems we decided to store the index in a Microsoft SQLServer database. In contrast to the Java JDBCDirectory this implementation is only aimed at storing it in MS SQLServer 2008 or higher, using it's advanced random read/write capabilities.

Please notice that this implementation only intends to give a solution to indexes for less than 2 GB and that for high & concurrent write and near realtime search we suggest to look at [ElasticSearch](https://www.elastic.co/) or [Solr](http://lucene.apache.org/solr/) or managed services such as [Azure Search](http://azure.microsoft.com/en-us/services/search/ "Azure Search").

## How to use ##
    using (var connection = new SqlConnection(@"......")) {
	connection.Open();

	var directory = new SqlServerDirectory(connection, new Options() { SchemaName = "[search]" });
	var exists = !IndexReader.IndexExists(directory);
	var indexWriter = new IndexWriter(directory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30),exists,new Lucene.Net.Index.IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH));

	var doc = new Document();
    doc.Add(new Field("id", "1", Field.Store.YES,Field.Index.ANALYZED, Field.TermVector.NO));
    doc.Add(new Field("Title", "this is my title", Field.Store.NO, Field.Index.ANALYZED,Field.TermVector.NO));
    indexWriter.AddDocument(doc);

	searcher = new IndexSearcher(directory);
	var hits = searcher.Search(new TermQuery(new Term("Title", "find me")), 100);


The SqlServerDirectory class takes in:

1. a connection (should be opened before because the implementation does not do any connection management such as disposing, opening or closing the given connection)
2. an Options instance in which the user can define in which database schema the required structure can be found (default = 'dbo' and a writer lock timeout (default 10 minutes)

The required database structure can be provisioned using the following codesnippet:

    using (var connection = new SqlConnection("....")){
	connection.Open();
	SqlServerDirectory.ProvisionDatabase(connection, schemaName: "[search]", dropExisting: true); 


## Limits ##
1. An individual index file cannot grow beyond 2GB.
1. There is a single writer locking factory included that is default used.
2. MultipleActiveResultSets should be enabled on the connection given to the SQLServerDirectory
  

## How to compile/contribute ##
The codebase is currently writen in C# 6.0 and has a solution file for Microsoft Visual Studio 2015. Any contribution is welcome.
