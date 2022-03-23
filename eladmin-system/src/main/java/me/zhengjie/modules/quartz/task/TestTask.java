/*
 *  Copyright 2019-2020 Zheng Jie
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package me.zhengjie.modules.quartz.task;

import cn.hutool.core.thread.ThreadUtil;
import com.alibaba.fastjson.JSON;
import com.cdos.utils.DateUtil;
import com.cdos.utils.Safes;
import com.cdos.utils.json.JacksonProvider;
import com.cdos.web.htppclient.CdosHttpRestTemplate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import me.zhengjie.modules.doum.enums.DateBetweenEnum;
import me.zhengjie.modules.doum.repository.AwemeRepository;
import me.zhengjie.modules.doum.repository.UserMonitorRepository;
import me.zhengjie.modules.doum.repository.dto.aweme.AwemeDetailDto;
import me.zhengjie.modules.doum.repository.dto.aweme.AwemeDto;
import me.zhengjie.modules.doum.repository.dto.aweme.AwemeListObjectDto;
import me.zhengjie.modules.doum.service.dto.AwemeExtraDto;
import me.zhengjie.modules.doum.service.dto.AwemeLikeQueryCriteria;
import me.zhengjie.modules.doum.service.dto.UserMonitorDto;
import me.zhengjie.modules.doum.service.impl.AwemeLikeServiceImpl;
import me.zhengjie.utils.RedisUtils;
import me.zhengjie.utils.SecurityUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 测试用
 *
 * @author Zheng Jie
 * @date 2019-01-08
 */
@Slf4j
@Async
@Component
public class TestTask {
    @Autowired
    CdosHttpRestTemplate awemeHttpRestTemplate;
    @Autowired
    UserMonitorRepository userMonitorRepository;
    @Autowired
    AwemeRepository awemeRepository;
    @Autowired
    AwemeLikeServiceImpl awemeLikeService;
    @Autowired
    RedisUtils redisUtils;

    public void run() {
        log.info("run 执行成功");
    }

    public void run1(String str) {
        log.info("run1 执行成功，参数为： {}" + str);
    }

    public void run2() {
        log.info("run2 执行成功");
    }

    /**
     * 爬虫任务
     */
    public void runAweme(String sec_user_id) {
        ResponseEntity<String> entity = awemeHttpRestTemplate.getDelegate()
            .getForEntity("http://localhost:8868/getAwemeListBySecUserId?sec_user_id=" + sec_user_id + "&current_user"
                + SecurityUtils.getCurrentUserId(), String.class);
        log.info("runAweme 执行完毕 response : {}", JSON.toJSONString(entity));
    }

    /**
     * 爬虫任务 -- 批量
     */
    public void batchRunAweme() {
        List<UserMonitorDto> userMonitorDtos = userMonitorRepository.listForPage(null, null, 200, UserMonitorDto.class);
        log.info("批量爬虫任务，当前size:{}", userMonitorDtos.size());

        for (int i = 0; i < userMonitorDtos.size(); i++) {

            log.info("当前执行到第{}/{}个用户，uid：{}，sec_user_id：{}", userMonitorDtos.size(), i, userMonitorDtos.get(i)
                .getUid(), userMonitorDtos.get(i)
                .getSec_user_id());
            try {
                ResponseEntity<String> entity = awemeHttpRestTemplate.getDelegate()
                    .getForEntity("http://localhost:8868/getAwemeListBySecUserId?sec_user_id=" + userMonitorDtos.get(i)
                        .getSec_user_id() + "&current_user=" + userMonitorDtos.get(i)
                        .getUser_id(), String.class);

                log.info("runAweme 执行完毕 response : {}", JSON.toJSONString(entity));
            } catch (Exception e) {
                log.info("当前执行到第{}/{}个用户，uid：{}，sec_user_id：{}", userMonitorDtos.size(), i, userMonitorDtos.get(i)
                    .getUid(), userMonitorDtos.get(i)
                    .getSec_user_id());
            }
        }
    }

    /**
     * 10位int型的时间戳转换为String(yyyy-MM-dd HH:mm:ss)
     *
     * @param time
     * @return
     */
    public static Date TimestampToDate(Integer time) {
        long temp = (long)time * 1000;
        Timestamp ts = new Timestamp(temp);
        Date date = new Date();
        try {
            date = ts;
        } catch (Exception e) {
            log.error("异常：{}", e.getMessage());
        }
        return date;
    }

    /**
     * 单线程/html
     */
    public void singleHtmlUserVideoList() {
        List<UserMonitorDto> userMonitorDtos =
            userMonitorRepository.listForPage(null, null, 999999999, UserMonitorDto.class);
        log.info("批量爬虫任务，当前size:{}", userMonitorDtos.size());

        for (int i = 0; i < userMonitorDtos.size(); i++) {

            log.info("当前执行到第{}/{}个用户，uid：{}，sec_user_id：{}", i, userMonitorDtos.size(), userMonitorDtos.get(i)
                .getUid(), userMonitorDtos.get(i)
                .getSec_user_id());
            try {

                Map<String, String> params = Maps.newHashMapWithExpectedSize(3);
                params.put("sec_uid", userMonitorDtos.get(i)
                    .getSec_user_id());
                params.put("max_cursor", "0");
                params.put("count", "20");

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
                headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON_UTF8));
                headers.set("token", "test");
                String jsonParam = JacksonProvider.getObjectMapper()
                    .writeValueAsString(Objects.nonNull(params) ? params : "");

                HttpEntity<String> requestEntity = new HttpEntity<>(jsonParam, headers);

                String s = awemeHttpRestTemplate.getDelegate()
                    .postForObject("http://127.0.0.1:9822/api/aweme/userVideoList", requestEntity, String.class);

                // 格式化
                AwemeDto awemeDto = JSON.parseObject(s, AwemeDto.class);
                List<me.zhengjie.modules.doum.service.dto.AwemeDto> AwemeDtoList = Safes.of(awemeDto.getData())
                    .parallelStream()
                    .flatMap(map -> {

                        List<me.zhengjie.modules.doum.service.dto.AwemeDto> collect = Safes.of(map.getAweme_list())
                            .parallelStream()
                            .map(map1 -> {

                                me.zhengjie.modules.doum.service.dto.AwemeDto.AwemeDtoBuilder awemeDtoBuilder =
                                    me.zhengjie.modules.doum.service.dto.AwemeDto.builder()
                                        .aweme_id(map1.getAweme_id())
                                        .desc(map1.getDesc())
                                        .digg_count(map1.getStatistics()
                                            .getDigg_count())
                                        .collect_count(map1.getStatistics()
                                            .getCollect_count())
                                        .comment_count(map1.getStatistics()
                                            .getComment_count())
                                        .share_count(map1.getStatistics()
                                            .getShare_count())
                                        .share_url(map1.getShare_url())
                                        .create_time(TimestampToDate(map1.getCreate_time()))
                                        .update_time(DateUtil.getCurrentDateTime())
                                        .author_user_id(map1.getAuthor_user_id())
                                        .nickname(map1.getAuthor()
                                            .getNickname())
                                        .unique_id(map1.getAuthor()
                                            .getUnique_id())
                                        .is_delete(map1.getStatus()
                                            .getIs_delete());
                                // TODO: 2022/3/2
                                //.user_id(SecurityUtils.getCurrentUserId())

                                // 商品信息
                                if (null != map1.getStatus() && null != map1.getStatus()
                                    .getWith_goods() && map1.getStatus()
                                    .getWith_goods()) {
                                    awemeDtoBuilder.with_goods(map1.getStatus()
                                        .getWith_goods())
                                        .product_url_list(map1.getAnchor_info()
                                            .getIcon()
                                            .getUrl_list());

                                    List<AwemeExtraDto> awemeExtraDtos = JSON.parseArray(map1.getAnchor_info()
                                        .getExtra(), AwemeExtraDto.class);

                                    awemeDtoBuilder.extra(awemeExtraDtos);
                                }
                                return awemeDtoBuilder.build();
                            })
                            .collect(Collectors.toList());

                        return collect.stream();
                    })
                    .collect(Collectors.toList());

                if (AwemeDtoList.size() > 0) {
                    log.info("爬虫成功入库");
                    String index = "dm_aweme" + "_" + DateUtil.formatDate(DateUtil.getCurrentDate());
                    awemeRepository.insert(AwemeDtoList, index);
                } else {
                    log.error("爬虫异常awemeDto：{}", AwemeDtoList);
                }
            } catch (Exception e) {
                log.error("异常{}/{}个用户，uid：{}，sec_user_id：{}", userMonitorDtos.size(), i, userMonitorDtos.get(i)
                    .getUid(), userMonitorDtos.get(i)
                    .getSec_user_id(), e);
            }
        }
        saveAwemeResult();
    }

    /**
     * 单线程/app
     */
    public void singleAppUserVideoList() {
        List<UserMonitorDto> userMonitorDtos =
            userMonitorRepository.listForPage(null, null, 999999999, UserMonitorDto.class);
        log.info("singleAppUserVideoList 【单线程】【挂车】任务，当前size:{}", userMonitorDtos.size());
        AtomicInteger atomicLoop = new AtomicInteger(0);
        AtomicInteger atomicLoop_bak = new AtomicInteger(0);
        for (UserMonitorDto userMonitorDto : userMonitorDtos) {
            //            sendRequest(userMonitorDto, "http://127.0.0.1:9822/api/aweme/appUserVideoList", atomicLoop, atomicLoop_bak);
            sendRequestOnlyUpdateGoods(userMonitorDto, "http://127.0.0.1:9822/api/aweme/appUserVideoList", atomicLoop,
                atomicLoop_bak, 2);
        }
        saveAwemeResult();
    }

    /**
     * 多线程/app
     */
    public void moreAppUserVideoList(String threadSize) {
        AtomicInteger atomicLoop = new AtomicInteger(0);
        AtomicInteger atomicLoop_bak = new AtomicInteger(0);
        moreThreadBatchAweme("http://127.0.0.1:9822/api/aweme/appUserVideoList", atomicLoop, atomicLoop_bak,
            Integer.parseInt(threadSize), 2);
        saveAwemeResult();
    }

    /**
     * 多线程/app/视频详情
     */
    public void moreAppVideoDetail(String threadSize) {
        AtomicInteger atomicLoop = new AtomicInteger(0);
        AtomicInteger atomicLoop_bak = new AtomicInteger(0);
        moreThreadBatchAweme("http://127.0.0.1:9822/api/aweme/videoDetail", atomicLoop, atomicLoop_bak,
            Integer.parseInt(threadSize), 2);
        saveAwemeResult();
    }

    /**
     * 多线程/html
     */
    public void moreHtmlUserVideoList(String threadSize) {
        AtomicInteger atomicLoop = new AtomicInteger(0);
        AtomicInteger atomicLoop_bak = new AtomicInteger(0);
        moreThreadBatchAweme("http://127.0.0.1:9822/api/aweme/userVideoList", atomicLoop, atomicLoop_bak,
            Integer.parseInt(threadSize), 1);
        saveAwemeResult();
    }

    public void moreThreadBatchAweme(String url, AtomicInteger atomicLoop, AtomicInteger bak_atomicLoop,
        Integer theeadSize, Integer type) {

        List<UserMonitorDto> userMonitorDtos =
            userMonitorRepository.listForPage(null, null, 999999999, UserMonitorDto.class);
        log.info("多线程数量：{}，当前size：{}", theeadSize, userMonitorDtos.size());

        long start = System.currentTimeMillis();
        ExecutorService executor = ThreadUtil.newExecutor(theeadSize);
        final CountDownLatch countDownLatch = new CountDownLatch(userMonitorDtos.size());
        for (UserMonitorDto user : userMonitorDtos) {
            executor.execute(new Task(user, countDownLatch, url, atomicLoop, bak_atomicLoop, type));
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            executor.shutdown();
        }
        long end = System.currentTimeMillis();
        System.out.println("多线程方法，耗时：" + (end - start) + "ms");
    }

    // TODO: 2022/3/21 只筛选挂车的视频
    public void moreThreadBatchAwemeDetail(String url, AtomicInteger atomicLoop, AtomicInteger bak_atomicLoop,
        Integer theeadSize) {

        List<UserMonitorDto> userMonitorDtos =
            userMonitorRepository.listForPage(null, null, 999999999, UserMonitorDto.class);
        log.info("多线程数量：{}，当前size：{}", theeadSize, userMonitorDtos.size());

        long start = System.currentTimeMillis();
        ExecutorService executor = ThreadUtil.newExecutor(theeadSize);
        final CountDownLatch countDownLatch = new CountDownLatch(userMonitorDtos.size());
        for (UserMonitorDto user : userMonitorDtos) {
            executor.execute(new Taskdetail("", countDownLatch, url, atomicLoop, bak_atomicLoop));
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            executor.shutdown();
        }
        long end = System.currentTimeMillis();
        System.out.println("多线程方法，耗时：" + (end - start) + "ms");
    }

    public void testAweme() {
        AwemeLikeQueryCriteria four = new AwemeLikeQueryCriteria();
        four.setDateBetweenEnum(DateBetweenEnum.FOUR_HOUR);
        awemeLikeService.saveResults(four);
    }

    /**
     * 存储点赞增长数据
     * 不同维度的放到不同的索引
     */
    public void saveAwemeResult() {

        try {

            long start = System.currentTimeMillis();

            AwemeLikeQueryCriteria two = new AwemeLikeQueryCriteria();
            two.setDateBetweenEnum(DateBetweenEnum.TWO_HOUR);
            awemeLikeService.saveResults(two);

            System.out.println("2小时统计完成，耗时：" + (System.currentTimeMillis() - start) + "ms");

            AwemeLikeQueryCriteria four = new AwemeLikeQueryCriteria();
            four.setDateBetweenEnum(DateBetweenEnum.FOUR_HOUR);
            awemeLikeService.saveResults(four);

            System.out.println("4小时统计完成，耗时：" + (System.currentTimeMillis() - start) + "ms");

            AwemeLikeQueryCriteria six = new AwemeLikeQueryCriteria();
            six.setDateBetweenEnum(DateBetweenEnum.SIX_HOUR);
            awemeLikeService.saveResults(six);

            System.out.println("6小时统计完成，耗时：" + (System.currentTimeMillis() - start) + "ms");

            AwemeLikeQueryCriteria twelve = new AwemeLikeQueryCriteria();
            twelve.setDateBetweenEnum(DateBetweenEnum.TWELVE_HOUR);
            awemeLikeService.saveResults(twelve);

            System.out.println("12小时统计完成，耗时：" + (System.currentTimeMillis() - start) + "ms");

            // TODO: 2022/3/4 先不要开
            AwemeLikeQueryCriteria hour24 = new AwemeLikeQueryCriteria();
            hour24.setDateBetweenEnum(DateBetweenEnum.ONE_DAY);
            awemeLikeService.saveResults(hour24);

            System.out.println("24小时统计完成，耗时：" + (System.currentTimeMillis() - start) + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 视频详情
     */
    public void videoDetail(String aweme_id, String url, AtomicInteger atomicLoop, AtomicInteger bak_atomicLoop) {
        String result = "";

        int incrementAndGet = atomicLoop.incrementAndGet();
        log.info("当前执行到第{}个用户，aweme_id：{}", incrementAndGet, aweme_id);
        try {
            Map<String, String> params = Maps.newHashMapWithExpectedSize(1);
            params.put("aweme_id", aweme_id);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
            headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON_UTF8));
            headers.set("token", "test");
            String jsonParam = JacksonProvider.getObjectMapper()
                .writeValueAsString(Objects.nonNull(params) ? params : "");

            HttpEntity<String> requestEntity = new HttpEntity<>(jsonParam, headers);

            result = awemeHttpRestTemplate.getDelegate()
                .postForObject(url, requestEntity, String.class);

        } catch (Exception e) {
            log.error("异常{}个用户，uid：{}，aweme_id：{}", incrementAndGet, aweme_id, e);
        }
        // 处理返回
        AwemeDetailDto awemeDetailDto = JSON.parseObject(result, AwemeDetailDto.class);

        if (null == awemeDetailDto || !awemeDetailDto.getSuccess()
            .equals("success")) {
            return;
        }

        AwemeListObjectDto map1 = awemeDetailDto.getData()
            .getAweme_detail();

        me.zhengjie.modules.doum.service.dto.AwemeDto.AwemeDtoBuilder awemeDtoBuilder =
            me.zhengjie.modules.doum.service.dto.AwemeDto.builder()
                .aweme_id(map1.getAweme_id())
                .desc(map1.getDesc())
                .digg_count(map1.getStatistics()
                    .getDigg_count())
                .collect_count(map1.getStatistics()
                    .getCollect_count())
                .comment_count(map1.getStatistics()
                    .getComment_count())
                .share_count(map1.getStatistics()
                    .getShare_count())
                .share_url(map1.getShare_url())
                .create_time(TimestampToDate(map1.getCreate_time()))
                .update_time(DateUtil.getCurrentDateTime())
                .author_user_id(map1.getAuthor_user_id())
                .nickname(map1.getAuthor()
                    .getNickname())
                .unique_id(map1.getAuthor()
                    .getUnique_id())
                .is_delete(map1.getStatus()
                    .getIs_delete());

        // 商品信息
        if (null != map1.getStatus() && null != map1.getStatus()
            .getWith_goods() && map1.getStatus()
            .getWith_goods()) {

            try {
                awemeDtoBuilder.with_goods(map1.getStatus()
                    .getWith_goods())
                    .product_url_list(map1.getAnchor_info()
                        .getIcon()
                        .getUrl_list());
                List<AwemeExtraDto> awemeExtraDtos = JSON.parseArray(map1.getAnchor_info()
                    .getExtra(), AwemeExtraDto.class);

                awemeDtoBuilder.extra(awemeExtraDtos);
            } catch (Exception e) {
                log.error("没有爬取到挂车内容：{},{}", map1.getAweme_id(), JSON.toJSONString(map1));
            }
        }

        String index = "dm_aweme" + "_" + DateUtil.formatDate(DateUtil.getCurrentDate());
        awemeRepository.insert(awemeDtoBuilder.build(), index);
        int incrementAndGet_bak = bak_atomicLoop.incrementAndGet();
        log.info("处理返回结果:{}", incrementAndGet_bak);
    }

    @SuppressWarnings("all")
    public void sendRequest(UserMonitorDto user, String url, AtomicInteger atomicLoop, AtomicInteger bak_atomicLoop) {

        String result = "";

        int incrementAndGet = atomicLoop.incrementAndGet();
        log.info("当前执行到第{}个用户，uid：{}，sec_user_id：{}", incrementAndGet, user.getUid(), user.getSec_user_id());
        try {
            Map<String, String> params = Maps.newHashMapWithExpectedSize(3);
            params.put("sec_uid", user.getSec_user_id());
            params.put("max_cursor", "0");
            params.put("count", "20");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
            headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON_UTF8));
            headers.set("token", "test");
            String jsonParam = JacksonProvider.getObjectMapper()
                .writeValueAsString(Objects.nonNull(params) ? params : "");

            HttpEntity<String> requestEntity = new HttpEntity<>(jsonParam, headers);

            result = awemeHttpRestTemplate.getDelegate()
                .postForObject(url, requestEntity, String.class);

        } catch (Exception e) {
            log.error("异常{}个用户，uid：{}，sec_user_id：{}{}", incrementAndGet, user.getUid(), user.getSec_user_id(), e);
        }
        // 处理返回
        AwemeDto awemeDto = JSON.parseObject(result, AwemeDto.class);

        if (null == awemeDto) {
            return;
        }

        List<me.zhengjie.modules.doum.service.dto.AwemeDto> AwemeDtoList = Safes.of(awemeDto.getData())
            .parallelStream()
            .flatMap(map -> {

                List<me.zhengjie.modules.doum.service.dto.AwemeDto> collect = Safes.of(map.getAweme_list())
                    .parallelStream()
                    .map(map1 -> {

                        me.zhengjie.modules.doum.service.dto.AwemeDto.AwemeDtoBuilder awemeDtoBuilder =
                            me.zhengjie.modules.doum.service.dto.AwemeDto.builder()
                                .aweme_id(map1.getAweme_id())
                                .desc(map1.getDesc())
                                .digg_count(map1.getStatistics()
                                    .getDigg_count())
                                .collect_count(map1.getStatistics()
                                    .getCollect_count())
                                .comment_count(map1.getStatistics()
                                    .getComment_count())
                                .share_count(map1.getStatistics()
                                    .getShare_count())
                                .share_url(map1.getShare_url())
                                .create_time(TimestampToDate(map1.getCreate_time()))
                                .update_time(DateUtil.getCurrentDateTime())
                                .author_user_id(map1.getAuthor_user_id())
                                .nickname(map1.getAuthor()
                                    .getNickname())
                                .unique_id(map1.getAuthor()
                                    .getUnique_id())
                                .is_delete(map1.getStatus()
                                    .getIs_delete());
                        // TODO: 2022/3/2
                        //.user_id(SecurityUtils.getCurrentUserId())

                        // 商品信息
                        if (null != map1.getStatus() && null != map1.getStatus()
                            .getWith_goods() && map1.getStatus()
                            .getWith_goods()) {

                            try {
                                awemeDtoBuilder.with_goods(map1.getStatus()
                                    .getWith_goods())
                                    .product_url_list(map1.getAnchor_info()
                                        .getIcon()
                                        .getUrl_list());
                                List<AwemeExtraDto> awemeExtraDtos = JSON.parseArray(map1.getAnchor_info()
                                    .getExtra(), AwemeExtraDto.class);

                                awemeDtoBuilder.extra(awemeExtraDtos);
                            } catch (Exception e) {
                                log.error("没有爬取到挂车内容：{},{}", map1.getAweme_id(), JSON.toJSONString(map1));
                            }

                        }
                        return awemeDtoBuilder.build();
                    })
                    .collect(Collectors.toList());

                return collect.stream();
            })
            .collect(Collectors.toList());

        if (AwemeDtoList.size() > 0) {
            String index = "dm_aweme" + "_" + DateUtil.formatDate(DateUtil.getCurrentDate());
            awemeRepository.insert(AwemeDtoList, index);
            int incrementAndGet_bak = bak_atomicLoop.incrementAndGet();
            log.info("处理返回结果:{}", incrementAndGet_bak);
        }
    }

    /**
     * 只更新销量和挂车
     */
    @SuppressWarnings("all")
    public void sendRequestOnlyUpdateGoods(UserMonitorDto user, String url, AtomicInteger atomicLoop,
        AtomicInteger bak_atomicLoop, Integer type) {

        String result = "";

        int incrementAndGet = atomicLoop.incrementAndGet();
        log.info("当前执行到第{}个用户，uid：{}，sec_user_id：{}", incrementAndGet, user.getUid(), user.getSec_user_id());
        try {
            Map<String, String> params = Maps.newHashMapWithExpectedSize(3);
            params.put("sec_uid", user.getSec_user_id());
            params.put("max_cursor", "0");
            params.put("count", "20");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
            headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON_UTF8));
            headers.set("token", "test");
            String jsonParam = JacksonProvider.getObjectMapper()
                .writeValueAsString(Objects.nonNull(params) ? params : "");

            HttpEntity<String> requestEntity = new HttpEntity<>(jsonParam, headers);

            result = awemeHttpRestTemplate.getDelegate()
                .postForObject(url, requestEntity, String.class);

        } catch (Exception e) {
            log.error("异常{}个用户，uid：{}，sec_user_id：{}{}", incrementAndGet, user.getUid(), user.getSec_user_id(), e);
        }
        // 处理返回
        AwemeDto awemeDto = JSON.parseObject(result, AwemeDto.class);

        if (null == awemeDto) {
            return;
        }

        List<me.zhengjie.modules.doum.service.dto.AwemeDto> AwemeDtoList = Safes.of(awemeDto.getData())
            .parallelStream()
            .flatMap(map -> {

                List<me.zhengjie.modules.doum.service.dto.AwemeDto> collect = Safes.of(map.getAweme_list())
                    .parallelStream()
                    .map(map1 -> {

                        me.zhengjie.modules.doum.service.dto.AwemeDto.AwemeDtoBuilder awemeDtoBuilder =
                            me.zhengjie.modules.doum.service.dto.AwemeDto.builder()
                                .aweme_id(map1.getAweme_id())
                                .desc(map1.getDesc())
                                .digg_count(map1.getStatistics()
                                    .getDigg_count())
                                .collect_count(map1.getStatistics()
                                    .getCollect_count())
                                .comment_count(map1.getStatistics()
                                    .getComment_count())
                                .share_count(map1.getStatistics()
                                    .getShare_count())
                                .share_url(map1.getShare_url())
                                .create_time(TimestampToDate(map1.getCreate_time()))
                                .update_time(DateUtil.getCurrentDateTime())
                                .author_user_id(map1.getAuthor_user_id())
                                .nickname(map1.getAuthor()
                                    .getNickname())
                                .unique_id(map1.getAuthor()
                                    .getUnique_id())
                                .is_delete(map1.getStatus()
                                    .getIs_delete());
                        // TODO: 2022/3/2
                        //.user_id(SecurityUtils.getCurrentUserId())

                        // 商品信息
                        if (null != map1.getStatus() && null != map1.getStatus()
                            .getWith_goods() && map1.getStatus()
                            .getWith_goods()) {

                            try {
                                awemeDtoBuilder.with_goods(map1.getStatus()
                                    .getWith_goods())
                                    .product_url_list(map1.getAnchor_info()
                                        .getIcon()
                                        .getUrl_list());
                                List<AwemeExtraDto> awemeExtraDtos = JSON.parseArray(map1.getAnchor_info()
                                    .getExtra(), AwemeExtraDto.class);

                                awemeDtoBuilder.extra(awemeExtraDtos);
                            } catch (Exception e) {
                                log.error("没有爬取到挂车内容：{},{}", map1.getAweme_id(), JSON.toJSONString(map1));
                            }

                        }
                        return awemeDtoBuilder.build();
                    })
                    .collect(Collectors.toList());

                return collect.stream();
            })
            .collect(Collectors.toList());

        String index = "dm_aweme" + "_" + DateUtil.formatDate(DateUtil.getCurrentDate());
        if (AwemeDtoList.size() > 0) {
            switch (type) {
                case 1:
                    // TODO: 2022/3/21 发现视频只要有一次挂车是true，则都是true
                    awemeRepository.insert(AwemeDtoList, index);
                    int incrementAndGet_bak = bak_atomicLoop.incrementAndGet();
                    log.info("处理返回结果:{}", incrementAndGet_bak);

                    break;
                case 2:

                    String date = DateUtil.formatDate(DateUtil.getCurrentDate(), DateUtil.DATE_FORMAT);
                    Map<Long, me.zhengjie.modules.doum.service.dto.AwemeDto> collect1 = AwemeDtoList.parallelStream()
                        .collect(Collectors.toMap(me.zhengjie.modules.doum.service.dto.AwemeDto::getAweme_id,
                            Function.identity(), (k1, k2) -> k2));

                    // TODO: 2022/3/21 查询的时候 指定条件
                    List<me.zhengjie.modules.doum.service.dto.AwemeDto> awemeDtos = awemeRepository.listForPage(date,
                        QueryBuilders.boolQuery()
                            .must(QueryBuilders.termsQuery("aweme_id", collect1.keySet())), null, 999999999,
                        me.zhengjie.modules.doum.service.dto.AwemeDto.class);

                    Map<Long, List<me.zhengjie.modules.doum.service.dto.AwemeDto>> collect = awemeDtos.parallelStream()
                        .collect(Collectors.groupingBy(me.zhengjie.modules.doum.service.dto.AwemeDto::getAweme_id));

                    for (Map.Entry<Long, List<me.zhengjie.modules.doum.service.dto.AwemeDto>> entry : collect.entrySet()) {

                        me.zhengjie.modules.doum.service.dto.AwemeDto newData = Safes.of(entry.getValue())
                            .parallelStream()
                            .sorted(Comparator.comparing(me.zhengjie.modules.doum.service.dto.AwemeDto::getUpdate_time)
                                .reversed())
                            .findFirst()
                            .orElse(null);

                        if (null != newData) {
                            if (collect1.containsKey(entry.getKey())) {
                                me.zhengjie.modules.doum.service.dto.AwemeDto map1 = collect1.get(entry.getKey());

                                // 商品信息
                                if (null != map1.getWith_goods() && map1.getWith_goods()) {

                                    newData.setWith_goods(map1.getWith_goods());
                                    newData.setProduct_url_list(map1.getProduct_url_list());
                                    newData.setExtra(map1.getExtra());
                                    newData.setUpdate_time(DateUtil.getCurrentDateTime());
                                    // todo es好像这样插入不是更新，是新增了一条数据，也无所谓反正将上一次html的内容赋值过来了
                                    awemeRepository.insert(newData, index);
                                    log.info("当前更新索引={},==== aweme_id ={},{},{},{}", index, entry.getKey(),
                                        map1.getWith_goods(), map1.getProduct_url_list(),
                                        JSON.toJSONString(map1.getExtra()));
                                }
                            }
                        }
                    }
                    break;
            }
            int incrementAndGet_bak = bak_atomicLoop.incrementAndGet();
            log.info("处理返回结果:{}", incrementAndGet_bak);
        }
    }

    class Task implements Runnable {

        private UserMonitorDto user;
        private CountDownLatch countDownLatch;
        private String url;
        private AtomicInteger atomicLoop;
        private AtomicInteger bak_atomicLoop;
        private Integer type;

        public Task(UserMonitorDto user, CountDownLatch countDownLatch, String url, AtomicInteger atomicLoop,
            AtomicInteger bak_atomicLoop, Integer type) {
            this.user = user;
            this.countDownLatch = countDownLatch;
            this.url = url;
            this.atomicLoop = atomicLoop;
            this.bak_atomicLoop = bak_atomicLoop;
            this.type = type;
        }

        @Override
        public void run() {
            try {
                sendRequest(user, url, atomicLoop, bak_atomicLoop);
                //                sendRequestOnlyUpdateGoods(user, url, atomicLoop, bak_atomicLoop, type);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    class Taskdetail implements Runnable {

        private String aweme_id;
        private CountDownLatch countDownLatch;
        private String url;
        private AtomicInteger atomicLoop;
        private AtomicInteger bak_atomicLoop;

        public Taskdetail(String aweme_id, CountDownLatch countDownLatch, String url, AtomicInteger atomicLoop,
            AtomicInteger bak_atomicLoop) {
            this.aweme_id = aweme_id;
            this.countDownLatch = countDownLatch;
            this.url = url;
            this.atomicLoop = atomicLoop;
            this.bak_atomicLoop = bak_atomicLoop;
        }

        @Override
        public void run() {
            try {
                videoDetail(aweme_id, url, atomicLoop, bak_atomicLoop);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        }
    }
}
