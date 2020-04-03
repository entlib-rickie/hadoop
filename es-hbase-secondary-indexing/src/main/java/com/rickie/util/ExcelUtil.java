package com.rickie.util;

import com.rickie.entity.Article;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExcelUtil {
    //读取exel，将文件内容打印出来
    public static void main(String[] args) throws IOException {
        List<Article> exceInfo = getExcelInfo();
    }

    public static List<Article>  getExcelInfo() throws IOException {
        FileInputStream fileInputStream = new FileInputStream("D:\\tmp\\articles.xlsx");
        //获取我们解析excel表格的对象
        XSSFWorkbook xssfSheets = new XSSFWorkbook(fileInputStream);
        //获取excel的第一个sheet页
        XSSFSheet sheetAt = xssfSheets.getSheetAt(0);
        //获取我们sheet页的最后一行的数字之，说白了就是看这个excel一共有多少行
        int lastRowNum = sheetAt.getLastRowNum();
        List<Article> articleList = new ArrayList<Article>();

        for(int i =1 ;i<=lastRowNum;i++){
            Article article = new Article();
            //获取我们一行行的数据
            XSSFRow row = sheetAt.getRow(i);
            //通过我们的row对象，解析里面一个个的字段
            XSSFCell title = row.getCell(0);
            XSSFCell from  = row.getCell(1);
            XSSFCell time = row.getCell(2);
            XSSFCell readCount = row.getCell(3);
            XSSFCell content = row.getCell(4);
            System.out.println(title.toString());
            article.setId(i+"");
            article.setTitle(title.toString());
            article.setContent(content.toString());
            article.setFrom(from.toString());
            article.setReadCounts(readCount.toString());
            article.setTimes(time.toString());
            articleList.add(article);
        }
        fileInputStream.close();
        return  articleList;
    }
}
