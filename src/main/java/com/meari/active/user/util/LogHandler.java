package com.meari.active.user.util;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * 日志解析类
 *
 * @author fudong
 */
@Slf4j
public class LogHandler {

    /**
     * 日志目录 Eg:/data/pps/license/
     */
    private String logDir;

    /**
     * 读取日志文件的数量，默认从昨天日志开始逐个向前读取
     */
    private int dealNum;

    /**
     * 日志命名格式 Eg:alert-image-yy-mm-dd.log.gz
     */
    private String dateFormat;

    /**
     * 匹配行数据标志 Eg:AlertImageController
     */
    private String lineFlag;

    /**
     * 匹配目标字符串正则 Eg:<userID>(\d+)<userToken>
     */
    private Pattern targetPattern;


    /**
     * 处理日志
     * @return  date : dataSet
     */
    public Map<String,Set<String>> deal(){
        Map<String,Set<String>> result = new TreeMap<>();
        this.findLogFile().forEach((date, filePath) -> result.put(date,getTarget(filePath)));
        return result;
    }

    /**
     * 读取文件获取目标字符串
     *
     * @param filePath  日志路径
     * @return          匹配到的字符串set
     */
    private Set<String> getTarget(String filePath) {
        Set<String> resSet = new HashSet<>();
        boolean isDel = false;
        File file = null;
        BufferedReader reader = null;
        log.info("----> 读取文件：{}", filePath);
        try {
            if (filePath.endsWith("gz")){
                filePath = this.unGzipFile(filePath);
                isDel = true;
            }
            file = new File(filePath);
            //设置读取缓存，以防日志文件过大导致内存溢出（默认5M）
            int sz = 5 * 1024 * 1024;
            reader = new BufferedReader(new FileReader(file), sz);
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains(lineFlag)){
                    Matcher m = targetPattern.matcher(line);
                    if (m.find()){
                        resSet.add(m.group(1));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (file != null && isDel){
                file.delete();
            }
        }
        return resSet;
    }

    /**
     * 获取文件夹下日志文件
     *
     * @return  date : logFilePath
     */
    private Map<String,String> findLogFile() {
        Map<String,String> fileAll = new TreeMap<>();
        long currentTime = System.currentTimeMillis();
        SimpleDateFormat logDateFormat = new SimpleDateFormat(dateFormat);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        for (int i=1;i<=dealNum;i++){
            long logTime = currentTime - i*24*60*60*1000;
            String fileName = logDir + logDateFormat.format(logTime);
            if (!new File(fileName).exists()){
                break;
            }
            String key = simpleDateFormat.format(logTime);
            fileAll.put(key,fileName);
        }
        return fileAll;
    }

    /**
     * 解压gz文件
     *
     * @return  解压后的文件路径
     * @throws Exception e
     */
    private String unGzipFile(String filePath) throws Exception {
        String ouputfile = "";
        //建立gzip压缩文件输入流
        FileInputStream fin = new FileInputStream(filePath);
        //建立gzip解压工作流
        GZIPInputStream gzin = new GZIPInputStream(fin);
        //建立解压文件输出流
        ouputfile = filePath.substring(0,filePath.lastIndexOf('.'));
        FileOutputStream fout = new FileOutputStream(ouputfile);
        int num;
        byte[] buf=new byte[1024*1024];
        while ((num = gzin.read(buf,0,buf.length)) != -1) {
            fout.write(buf,0,num);
        }
        gzin.close();
        fout.close();
        fin.close();
        return ouputfile;
    }


    public LogHandler setLogDir(String logDir) {
        this.logDir = logDir;
        return this;
    }

    public LogHandler setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
        return this;
    }

    public LogHandler setTargetPattern(String targetPattern) {
        this.targetPattern = Pattern.compile(targetPattern);
        return this;
    }

    public LogHandler setLineFlag(String lineFlag) {
        this.lineFlag = lineFlag;
        return this;
    }

    public LogHandler setDealNum(int dealNum) {
        this.dealNum = dealNum;
        return this;
    }

    public String getLogDir() {
        return logDir;
    }

}
