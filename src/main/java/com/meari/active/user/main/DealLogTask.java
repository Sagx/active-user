package com.meari.active.user.main;

import com.meari.active.user.util.LogHandler;
import com.meari.active.user.util.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;

/**
 * 活跃用户统计任务类
 * 统计方法：解析日志，提取调用设备列表接口和报警消息接口的用户id，并以日期为单位以bitmap的方式存入redis
 * 图表展示：详见meari后台管理系统userManager/activeUser页面
 *
 * @author fudong
 */
@Slf4j
@Component
public class DealLogTask implements CommandLineRunner {

    @Value("${tomcat.log.path}")
    private String tomcatLogPath;
    @Value("${alert.log.path}")
    private String alertImageLogPath;

    @Resource
    private RedisClient redisClient;

    /**
     * task
     * @param args 不传时默认只处理昨天的日志，传一个参数代表处理前args[0]天的日志
     * @throws Exception e
     */
    @Override
    public void run(String... args) throws Exception {
        log.info("Task start ...");
        long startTime = System.currentTimeMillis();
        int dealNum = 1;
        if (args != null && args.length > 0){
            dealNum = Integer.parseInt(args[0]);
        }
        List<LogHandler> logHandlerList = new ArrayList<>();
        //tomcat日志(提取获取设备列表接口中的userId)
        logHandlerList.add(new LogHandler()
                .setLogDir(tomcatLogPath)
                .setDateFormat("'pps'.yyyyMMdd.'log'")
                .setDealNum(dealNum)
                .setLineFlag("<getDevice>")
                .setTargetPattern("<userID>(\\d+)<userToken>"));
        //报警图片日志(提取获取报警消息接口中的userId)
        logHandlerList.add(new LogHandler()
                .setLogDir(alertImageLogPath)
                .setDateFormat("'alert-image'-yyyy-MM-dd.'log.gz'")
                .setDealNum(dealNum)
                .setLineFlag("<selectMessageByDeviceUUID>")
                .setTargetPattern("<userID>(\\d+)<sourceApp>"));
        //将获取到的活跃用户数据存入redis bitMap
        logHandlerList.forEach( logHandler -> {
            log.info("--> 日志路径：{}",logHandler.getLogDir());
            logHandler.deal().forEach( (date,dataSet) -> {
                log.info("----> 日期：{}，用户数据量：{}", date, dataSet.size());
                int insertNum = 0;
                for (String userIdStr : dataSet) {
                    long userId = Long.parseLong(userIdStr) - 10000000;
                    //1.只统计userId大于10000000的数据
                    //2.用户暂时限制到十万以防出现异常数据bit过大导致占用过多内存
                    //3.使用bitmap存储是为了方便的统计月活跃用户，以及应对用户增长带来的统计数据存储空间占用
                    if (userId >= 0 && userId < 100000) {
                        if (!redisClient.setBit("AU" + date, userId)){
                            insertNum++;
                        }
                    }
                }
                log.info("------> 活跃用户：{}", insertNum);
            });
        });
        long endTime = System.currentTimeMillis();
        log.info("Spend Time:" + (endTime-startTime)/1000 + "s");
        log.info("Task end!");
    }

}
