package com.gugu.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;


public class HelloWorldTest {

    // 索引位置
    public static final String indexPath = "D:\\home\\gugu\\Documents\\dev\\lucene\\index";

    /**
     * @Description 往用lucene写入数据
     * @params
     * @return void
     * @auther gugu
     */

    @Test
    public void testCreate() throws Exception{
        Article article = new Article();
//        article.setId(1L);
//        article.setAuthor("gugu");
//        article.setTitle("Redis缓存穿透问题及解决方案");
//        article.setContent("Redis缓存穿透问题及解决方案\n" +
//                "场景：查询信息的时候，缓存并未找到对应信息，则查数据库为空，且不会加入缓存，这就会导致，下次在查询同样信息，由于缓存未命中，则仍旧会查底层数据库，所以缓存就一直未起到应有的作用，当并发流量大时，会很容易把DB打垮。！");
//        article.setUrl("https://blog.csdn.net/zhazhagu/article/details/107905280");
        article.setId(10L);
        article.setAuthor("gugu");
        article.setTitle("Redis入门");
        article.setContent("Redis入门1212122啊飒飒大师大入门");
        article.setUrl("https://blog.csdn.net/zhazhagu/article/details/121212121454");

        FSDirectory fsDirectory = FSDirectory.open(Paths.get(indexPath));
        // 创建标准分词器
//        Analyzer analyzer = new StandardAnalyzer();
        Analyzer analyzer = new IKAnalyzer(true);

        // 写入索引配置，指定分词器
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        IndexWriter indexWriter = new IndexWriter(fsDirectory, indexWriterConfig);
        // 创建文档对象
        Document document = article.toDocument();
        indexWriter.addDocument(document);
        indexWriter.close();
    }

    @Test
    public void testSearch() throws Exception{
        // 注意需要与创建索引使用同一个分词器
//        Analyzer analyzer = new StandardAnalyzer();
        Analyzer analyzer = new IKAnalyzer(true);
        DirectoryReader directoryReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        // 索引查询器
        IndexSearcher indexSearcher = new IndexSearcher(directoryReader);

        String str = "Redis";
        // 创建一个查询条件解析器
        QueryParser parser = new QueryParser("title", analyzer);

//        String str = "gu";
//        // 创建一个查询条件解析器
//        QueryParser parser = new QueryParser("author", analyzer);

        //对查询条件进行解析
        Query query = parser.parse(str);

//        //TermQuery将查询条件当成是一个固定的词
//        Query query = new TermQuery(new Term("url", "https://blog.csdn.net/zhazhagu/article/details/107905280"));

        //在【索引】中进行查找
        TopDocs topDocs = indexSearcher.search(query, 10);
        // 获取到查找到的文文档ID和得分
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            // 从索引中查询到文档的ID，
            int doc = scoreDoc.doc;
            //在根据ID到文档中查找文档内容
            Document document = indexSearcher.doc(doc);
            // 将文档转换成对应的实体类
            Article article = Article.parseArticle(document);
            System.out.println(article);
        }

        directoryReader.close();
    }

    @Test
    public void testDelete() throws IOException, ParseException {
        Analyzer analyzer = new StandardAnalyzer();
        FSDirectory fsDirectory = FSDirectory.open(Paths.get(indexPath));
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);

        IndexWriter indexWriter = new IndexWriter(fsDirectory, indexWriterConfig);
        //Term词条查找，内容必须完全匹配，不分词
        //indexWriter.deleteDocuments(new Term("content", "缓存"));

        //QueryParser parser = new QueryParser("title", analyzer);
        //Query query = parser.parse("解决方案");

        //LongPoint是建立索引的
        //Query query = LongPoint.newRangeQuery("id", 1L, 100L);
        Query query = LongPoint.newExactQuery("id", 1L);
        indexWriter.deleteDocuments(query);

        indexWriter.commit();
        indexWriter.close();
    }

    /**
     * @Description lucene的update比较特殊，update的代价太高，先删除，然后在插入
     * @params
     * @return void
     * @auther gugu
     */
    @Test
    public void testUpdate() throws Exception {
        Analyzer analyzer = new StandardAnalyzer();
        FSDirectory fsDirectory = FSDirectory.open(Paths.get(indexPath));
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        IndexWriter indexWriter = new IndexWriter(fsDirectory, indexWriterConfig);

        Article article = new Article();
        article.setId(1L);
        article.setAuthor("gugu");
        article.setTitle("Redis缓存穿透问题及解决方案");
        article.setContent("Redis缓存穿透问题及解决方案\n" +
                "场景：查询信息的时候，缓存并未找到对应信息，则查数据库为空，且不会加入缓存，这就会导致，下次在查询同样信息，由于缓存未命中，则仍旧会查底层数据库，所以缓存就一直未起到应有的作用，当并发流量大时，会很容易把DB打垮。！");
        article.setUrl("https://blog.csdn.net/zhazhagu/article/details/107905280");
        Document document = article.toDocument();

        indexWriter.updateDocument(new Term("author", "小谷"), document);
        indexWriter.commit();
        indexWriter.close();
    }

    @Test
    public void testMultiField() throws Exception{
        Analyzer analyzer = new StandardAnalyzer();
        DirectoryReader directoryReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));

        IndexSearcher indexSearcher = new IndexSearcher(directoryReader);

        String[] fields = {"title", "author", "content"};
        //多字段的查询转换器
        MultiFieldQueryParser queryParser = new MultiFieldQueryParser(fields, analyzer);
        Query query = queryParser.parse("Redis");

        TopDocs topDocs = indexSearcher.search(query, 10);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            int doc = scoreDoc.doc;
            Document document = indexSearcher.doc(doc);
            Article article = Article.parseArticle(document);
            System.out.println(article);
        }

        directoryReader.close();
    }
        
    @Test
    public void testMatchAll() throws Exception{
        Analyzer analyzer = new StandardAnalyzer();
        DirectoryReader directoryReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));

        IndexSearcher indexSearcher = new IndexSearcher(directoryReader);
        Query query = new MatchAllDocsQuery();
        TopDocs topDocs = indexSearcher.search(query, 10);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            int doc = scoreDoc.doc;
            Document document = indexSearcher.doc(doc);
            Article article = Article.parseArticle(document);
            System.out.println(article);
        }
        directoryReader.close();
    }

    /**
     * @Description 布尔查询，可以组合多个查询条件
     * @params
     * @return void
     * @auther gugu
     */
    @Test
    public void testBooleanQuery() throws Exception{
        DirectoryReader directoryReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher indexSearcher = new IndexSearcher(directoryReader);

        Query query1 = new TermQuery(new Term("title", "Redis"));
        Query query2 = new TermQuery(new Term("content", "缓存"));

        BooleanClause booleanClause1 = new BooleanClause(query1, BooleanClause.Occur.MUST);
        BooleanClause booleanClause2 = new BooleanClause(query2, BooleanClause.Occur.MUST_NOT);

        BooleanQuery booleanClauses = new BooleanQuery.Builder().add(booleanClause1).add(booleanClause2).build();
        System.out.println(booleanClauses);

        TopDocs topDocs = indexSearcher.search(booleanClauses, 10);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            int doc = scoreDoc.doc;
            Document document = indexSearcher.doc(doc);
            Article article = Article.parseArticle(document);
            System.out.println(article);
        }
        directoryReader.close();
    }

    @Test
    public  void testQueryParser() throws Exception {
        DirectoryReader directoryReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher indexSearcher = new IndexSearcher(directoryReader);

        //创建一个QueryParser对象。参数1：默认搜索域 参数2：分析器对象。
        QueryParser queryParser = new QueryParser("title", new StandardAnalyzer());

        //Query query = queryParser.parse("数据");
        Query query = queryParser.parse("title:Redis OR title:缓存");
        System.out.println(query);

        TopDocs topDocs = indexSearcher.search(query, 10);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            int doc = scoreDoc.doc;
            Document document = indexSearcher.doc(doc);
            Article article = Article.parseArticle(document);
            System.out.println(article);
        }
        directoryReader.close();
    }

    @Test
    public void testRangeQuery() throws Exception{
        DirectoryReader directoryReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher indexSearcher = new IndexSearcher(directoryReader);

        Query query = LongPoint.newRangeQuery("id", 1L, 10L);
        System.out.println(query);
        TopDocs topDocs = indexSearcher.search(query, 10);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            int doc = scoreDoc.doc;
            Document document = indexSearcher.doc(doc);
            Article article = Article.parseArticle(document);
            System.out.println(article);
        }
        directoryReader.close();
    }

}
