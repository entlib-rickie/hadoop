package com.rickie.controller;

import com.rickie.entity.Article;
import com.rickie.service.ArticleService;
import com.rickie.util.ExcelUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("article")
public class ArticleController {

    @Autowired
    private ArticleService articleService;

    @GetMapping("saveToEs")
    public String saveToEs(){
        try{
            // 使用Java代码解析Excel表格
            List<Article> lstArticle = ExcelUtil.getExcelInfo();

            // 将集合中的数据，保存到es中去
            articleService.saveToEs(lstArticle);

            return "Save to Elasticsearch";
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "Failed.";
    }

    @GetMapping("saveToHbase")
    public String saveToHbase(){
        try {
            // 使用Java代码解析Excel表格
            List<Article> lstArticle = ExcelUtil.getExcelInfo();

            articleService.saveToHbase(lstArticle);
            return "Save to HBase";
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "Failed.";
    }

    @GetMapping("queryByRowkey/{rowkey}")
    public Article queryByRowkey(@PathVariable String rowkey) {
        return articleService.getFromHbase(rowkey);
    }

    @GetMapping("queryByTitle/{title}")
    public List<Article> queryByTitle(@PathVariable String title) {
        return articleService.queryByTitle(title);
    }

    @GetMapping("getOnlyOne/{title}")
    public Article getOnlyOne(@PathVariable String title) {
        return articleService.getOnlyOne(title);
    }

    @GetMapping("queryBySecondaryIndex/{title}")
    public Article getOneFullArticleByTitle(@PathVariable String title) {
        return articleService.getOneFullArticleFromHbase(title);
    }

    @GetMapping("getMoreBySecondaryIndex/{title}")
    public List<Article> getMoreFullArticlesByTitle(@PathVariable String title) {
        return articleService.getMultiFullArticlesFromHbase(title);
    }
}
