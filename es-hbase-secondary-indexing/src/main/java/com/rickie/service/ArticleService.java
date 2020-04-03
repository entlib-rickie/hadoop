package com.rickie.service;

import com.rickie.entity.Article;
import com.rickie.entity.EsEntity;
import com.rickie.es.EsUtil;
import com.rickie.hbase.api.HBaseConn;
import com.rickie.hbase.api.HBaseUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ArticleService {
    private static final String tableName = "hbase_articles";
    private static final String familyName = "base";
    private static final String title = "title";
    private static final String from = "from";
    private static final String times ="times";
    private static final String readCounts =  "readCounts";
    private static final String content ="content";
    private static final String es_index = "articles";

    public void saveToHbase(List<Article> articleList) throws IOException {
        // Create Table
        HBaseUtil.createTable(tableName, new String[] {familyName});

        Table table = HBaseConn.getTable(tableName);
        System.out.println(articleList.size());
        long startTime = System.currentTimeMillis();
        List<Put> putList = new ArrayList<Put>();
        for (Article article : articleList) {
            System.out.println(article.getTitle());
            Put put = new Put(Bytes.toBytes(article.getId()));
            if(article.getTitle() != null   &&  article.getTitle() != ""){
                put.addColumn(familyName.getBytes(),title.getBytes(),article.getTitle().getBytes());
                put.addColumn(familyName.getBytes(),from.getBytes(),article.getFrom().getBytes());
                put.addColumn(familyName.getBytes(),times.getBytes(),article.getTimes().getBytes());
                put.addColumn(familyName.getBytes(),readCounts.getBytes(),article.getReadCounts().getBytes());
                put.addColumn(familyName.getBytes(),content.getBytes(),article.getContent().getBytes());
                putList.add(put);
            }
        }
        table.put(putList);
        long endTime = System.currentTimeMillis();
        System.out.println((endTime-startTime));
        table.close();
    }

    @Autowired
    private EsUtil<Article> esUtil;

    public void saveToEs(List<Article> articleList) {
        List<EsEntity> lst = new ArrayList<>();
        articleList.forEach(item->lst.add(
                new EsEntity(String.valueOf(item.getId()), item))
        );
        //通过批量添加，将我们的数据保存到es当中去
        Map<String, String> result = esUtil.insertBatch(es_index, lst);
    }

    public List<Article> queryByTitle(String qTitle) {
        ArrayList<String> lstId = new ArrayList<>();
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(new TermQueryBuilder(title, qTitle));
        builder.size(5);
        List<Article> articles = esUtil.search(es_index, builder, Article.class);

        if(articles != null && articles.size() > 0) {
            articles.forEach(item->lstId.add(item.getId()));
        }

        return articles;
    }

    public Article getOnlyOne(String qTitle) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("title", qTitle);
        matchQueryBuilder.operator(Operator.AND);
        builder.query(matchQueryBuilder);
        builder.size(1);

        List<Article> articles = esUtil.search(es_index, builder, Article.class);
        if(articles != null && articles.size() >0) {
            return articles.get(0);
        } else {
            return null;
        }
    }

    public Article getFromHbase(String rowKey) {
        Result result = HBaseUtil.getRow(tableName, rowKey);
        if(result != null && result.rawCells().length >0) {
            Article article = new Article();
            article.setId(Bytes.toString(result.getRow()));
            article.setTitle(Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(title))));
            article.setFrom(Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(from))));
            article.setTimes(Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(times))));
            article.setReadCounts(Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(readCounts))));
            article.setContent(Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(content))));
            System.out.println(article.toString());

            return article;
        } else {
            return null;
        }
    }

    public List<Article> getMoreFromHbase(List<String> lstRowkey) {
        List<Result> results = HBaseUtil.getRows(tableName, lstRowkey);
        List<Article> articles = new ArrayList<>();
        for (Result result : results) {
            if (result != null && result.rawCells().length > 0) {
                Article article = new Article();
                article.setId(Bytes.toString(result.getRow()));
                article.setTitle(Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(title))));
                article.setFrom(Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(from))));
                article.setTimes(Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(times))));
                article.setReadCounts(Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(readCounts))));
                article.setContent(Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(content))));
                System.out.println(article.toString());

                articles.add(article);
            }
        }

        return articles;
    }

    public Article getOneFullArticleFromHbase(String title){
        Article article = getOnlyOne(title);
        article = getFromHbase(article.getId());

        return article;
    }

    public List<Article> getMultiFullArticlesFromHbase(String title) {
        List<Article> articles = queryByTitle(title);
        List<String> lstString = new ArrayList<>();
        for(Article article : articles) {
            if(article != null) {
                lstString.add(article.getId());
            }
        }
        articles = getMoreFromHbase(lstString);

        return articles;
    }
}
