package com.meari.active.user.main;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * @author fudong
 */
@Slf4j
@Component
public class DealData implements CommandLineRunner {

    @Value("${tomcat.log.path}")
    private String DEVICE_LOG_PATH;
    @Value("${alert.log.path}")
    private String MESSAGE_LOG_PATH;

    //取验证码正则
    private static Pattern devicePattern = Pattern.compile("<userID>(\\d+)<userToken>");  //m.group(1)
    private static Pattern messagePattern = Pattern.compile("<userID>(\\d+)<sourceApp>");
    private final String DEVICE_API_FLAG1 = "DeviceAction";
    private final String DEVICE_API_FLAG2 = "<getDevice><phoneType>";
    private final String MESSAGE_API_FLAG1 = "AlertImageController";
    private final String MESSAGE_API_FLAG2 = "<selectMessageByDeviceUUID><deviceID>";
    private final String ACTIVE_USER_KEY = "AU";
    private final String RISE_USER_KEY = "RU";

    //文件读取缓存5M
    private final int CATCH_SIZE = 5 * 1024 * 1024;

    @Resource
    private JdbcTemplate jdbcTemplate;
    @Resource
    private RedisTemplate<String, String> redisTemplate;
    //序列化
    @Autowired(required = false)
    public void setRedisTemplate(RedisTemplate redisTemplate) {
        RedisSerializer stringSerializer = new StringRedisSerializer();
        redisTemplate.setKeySerializer(stringSerializer);
        redisTemplate.setValueSerializer(stringSerializer);
        redisTemplate.setHashKeySerializer(stringSerializer);
        redisTemplate.setHashValueSerializer(stringSerializer);
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Task start ...");
        long startTime = System.currentTimeMillis();
        List<String> deviceFilePath = new ArrayList<>();
        List<String> messageFilePath = new ArrayList<>();
        //获取要解析的文件
        if (args.length>0 && "all".equals(args[0])){
            dealRiseUser(30);
            deviceFilePath = findLogFile(DEVICE_LOG_PATH);
            messageFilePath = findLogFile(MESSAGE_LOG_PATH);
        }else{
            dealRiseUser(1);
            deviceFilePath.add(DEVICE_LOG_PATH +"pps."+new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis()-1000*60*60*24)+".log");
            messageFilePath.add(MESSAGE_LOG_PATH + "alert-image-"+new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis()-1000*60*60*24)+".log.gz");
        }
        //解析文件
        dealTomcatLog(deviceFilePath);
        dealAlertImageLog(messageFilePath);

        //BitSet bitSet = BitSet.valueOf(redisTemplate.execute((RedisCallback<byte[]>) connection -> connection.get("AU20190416".getBytes())));
        //log.info("日活："+bitSet.cardinality());

        long endTime = System.currentTimeMillis();
        log.info("Spend Time:" + (endTime-startTime)/1000 + "s");
    }

    //处理增长用户
    private void dealRiseUser(int days) {
        long now = System.currentTimeMillis();
        for (int i=1;i<=days;i++){
            String dateString = new SimpleDateFormat("yyyyMMdd").format(now-1000*60*60*24*i);
            String sql = "select count(1) from USERBASEINFO WHERE DATE_FORMAT(CREATE_DATE,'%Y%m%d') = '" + dateString + "'";
            int riseNum = jdbcTemplate.queryForObject(sql,Integer.class);
            redisTemplate.opsForValue().set(RISE_USER_KEY+dateString,String.valueOf(riseNum));
            log.info(dateString + " rise user:" + riseNum);
        }
    }

    public static void main(String[] args) {
        BitSet bitSet = new BitSet();
        System.out.println(bitSet.cardinality());
        bitSet.set(2);
        bitSet.set(66);
        long[] aa = bitSet.toLongArray();
        System.out.println(aa);
    }

    //解析tomcat日志
    private void dealTomcatLog(List<String> deviceFilePath) {
        log.info("FileNum:"+deviceFilePath.size());
        for (String filePath : deviceFilePath){
            log.info("filePath:" + filePath);
            BufferedReader reader = null;
            HashSet<Long> hashSet = new HashSet<>();
            try {
                File file = new File(filePath);
                reader = new BufferedReader(new FileReader(file), CATCH_SIZE);
                String line;
                long matchNum = 0;
                long activeNum = 0;
                while ((line = reader.readLine()) != null) {
                    if (line.contains(DEVICE_API_FLAG1) && line.contains(DEVICE_API_FLAG2)){
                        Matcher m = devicePattern.matcher(line);
                        if (m.find()){
                            matchNum++;
                            long userId = Long.parseLong(m.group(1));
                            //log.info("userId:"+userId);
                            String date = filePath.substring(filePath.indexOf("pps.")+4,filePath.indexOf("pps.")+12);
                            if (hashSet.add(userId) && userId>=10000000 && userId < 10100000){
                                activeNum++;
                                redisTemplate.opsForValue().setBit(ACTIVE_USER_KEY+date,userId-10000000,true);
                            }
                        }
                    }
                }
                reader.close();
                log.info("matchNum:" + matchNum);
                log.info("activeNum:" + activeNum);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    //解析报警图片日志
    private void dealAlertImageLog(List<String> alertFilePath) {
        log.info("FileNum:"+alertFilePath.size());
        for (String filePath : alertFilePath){
            if (filePath.endsWith("gz")){
                filePath = unGzipFile(filePath);
            }
            if (filePath == null) {
                continue;
            }
            log.info("filePath:" + filePath);
            BufferedReader reader = null;
            HashSet<Long> hashSet = new HashSet<>();
            try {
                File file = new File(filePath);
                reader = new BufferedReader(new FileReader(file), CATCH_SIZE);
                String line;
                long matchNum = 0;
                long activeNum = 0;
                while ((line = reader.readLine()) != null) {
                    if (line.contains(MESSAGE_API_FLAG1) && line.contains(MESSAGE_API_FLAG2)){
                        Matcher m = messagePattern.matcher(line);
                        if (m.find()){
                            matchNum++;
                            long userId = Long.parseLong(m.group(1));
                            //log.info("userId:"+userId);
                            String date = filePath.substring(filePath.indexOf(".log")-10,filePath.indexOf(".log")).replace("-","");
                            if (hashSet.add(userId) && userId>=10000000 && userId < 10100000){
                                activeNum++;
                                redisTemplate.opsForValue().setBit(ACTIVE_USER_KEY+date,userId-10000000,true);
                            }
                        }
                    }
                }
                reader.close();
                file.delete();
                log.info("matchNum:" + matchNum);
                log.info("activeNum:" + activeNum);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    //获取文件夹下日志文件
    private List<String> findLogFile(String filepath) {
        List<String> fileList = new ArrayList<>();
        try {
            File file = new File(filepath);
            if (file.isDirectory()) {
                String[] filelist = file.list();
                if (filelist != null) {
                    for (String afile : filelist) {
                        File readfile = new File(filepath + afile);
                        if (!readfile.isDirectory() && afile.contains("20")) {
                            fileList.add(filepath + afile);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("findLogFile Exception:" + e.getMessage());
        }
        return fileList;
    }

    //解压gz文件
    private static String unGzipFile(String sourcedir) {
        String ouputfile = "";
        try {
            //建立gzip压缩文件输入流
            FileInputStream fin = new FileInputStream(sourcedir);
            //建立gzip解压工作流
            GZIPInputStream gzin = new GZIPInputStream(fin);
            //建立解压文件输出流
            ouputfile = sourcedir.substring(0,sourcedir.lastIndexOf('.'));
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
        } catch (Exception e){
            System.err.println(e.toString());
        }
        return null;
    }

}
