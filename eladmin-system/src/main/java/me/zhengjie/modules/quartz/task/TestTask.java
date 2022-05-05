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
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.zhengjie.modules.doum.enums.DateBetweenEnum;
import me.zhengjie.modules.doum.repository.*;
import me.zhengjie.modules.doum.repository.dto.aweme.AwemeDataDto;
import me.zhengjie.modules.doum.repository.dto.aweme.AwemeDetailDto;
import me.zhengjie.modules.doum.repository.dto.aweme.AwemeDto;
import me.zhengjie.modules.doum.repository.dto.aweme.AwemeListObjectDto;
import me.zhengjie.modules.doum.service.dto.AwemeExtraDto;
import me.zhengjie.modules.doum.service.dto.AwemeLikeQueryCriteria;
import me.zhengjie.modules.doum.service.dto.UserMonitorDto;
import me.zhengjie.modules.doum.service.impl.AwemeLikeServiceImpl;
import me.zhengjie.utils.RedisUtils;
import me.zhengjie.utils.SecurityUtils;
import me.zhengjie.utils.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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
@RequiredArgsConstructor
public class TestTask {
    private final CdosHttpRestTemplate awemeHttpRestTemplate;
    private final UserMonitorRepository userMonitorRepository;
    private final AwemeRepository awemeRepository;
    private final AwemeLikeServiceImpl awemeLikeService;
    private final RedisUtils redisUtils;

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

            Long uid = userMonitorDtos.get(i)
                .getUid();
            log.info("当前执行到第{}/{}个用户，uid：{}，sec_user_id：{}", i, userMonitorDtos.size(), uid, userMonitorDtos.get(i)
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
                                        .uid(uid)
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
            sendRequest(userMonitorDto, "http://127.0.0.1:9822/api/aweme/appUserVideoList", atomicLoop, atomicLoop_bak);
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
            Integer.parseInt(threadSize));
        saveAwemeResult();
    }

    /**
     * 多线程/html
     */
    public void moreHtmlUserVideoList(String threadSize) {
        AtomicInteger atomicLoop = new AtomicInteger(0);
        AtomicInteger atomicLoop_bak = new AtomicInteger(0);
        moreThreadBatchAwemeHTML("http://127.0.0.1:9822/api/aweme/userVideoList", atomicLoop, atomicLoop_bak,
            Integer.parseInt(threadSize));
        saveAwemeResult();
    }

    /**
     * 多线程/redis
     */
    public void moreRedis(String threadSize) {
        AtomicInteger atomicLoop = new AtomicInteger(0);
        AtomicInteger atomicLoop_bak = new AtomicInteger(0);
        moreRedis(atomicLoop, atomicLoop_bak, Integer.parseInt(threadSize));
    }

    /**
     * 多线程/app/视频详情
     */
    @Deprecated
    public void moreAppVideoDetail(String threadSize) {
        AtomicInteger atomicLoop = new AtomicInteger(0);
        AtomicInteger atomicLoop_bak = new AtomicInteger(0);
        moreThreadBatchAweme("http://127.0.0.1:9822/api/aweme/videoDetail", atomicLoop, atomicLoop_bak,
            Integer.parseInt(threadSize));
        saveAwemeResult();
    }

    public void moreThreadBatchAweme(String url, AtomicInteger atomicLoop, AtomicInteger bak_atomicLoop,
        Integer theeadSize) {

        List<UserMonitorDto> userMonitorDtos =
            userMonitorRepository.listForPage(null, null, 999999999, UserMonitorDto.class);
        log.info("多线程数量：{}，当前size：{}", theeadSize, userMonitorDtos.size());

        long start = System.currentTimeMillis();
        ExecutorService executor = ThreadUtil.newExecutor(theeadSize);
        final CountDownLatch countDownLatch = new CountDownLatch(userMonitorDtos.size());
        for (UserMonitorDto user : userMonitorDtos) {
            executor.execute(new Task(user, countDownLatch, url, atomicLoop, bak_atomicLoop));
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

    @SuppressWarnings("all")
    public void moreThreadBatchAwemeHTML(String url, AtomicInteger atomicLoop, AtomicInteger bak_atomicLoop,
        Integer theeadSize) {

        List<UserMonitorDto> userMonitorDtos =
            userMonitorRepository.listForPage(null, null, 999999999, UserMonitorDto.class);
        log.info("多线程数量：{}，当前size：{}", theeadSize, userMonitorDtos.size());

        long start = System.currentTimeMillis();
        ExecutorService executor = ThreadUtil.newExecutor(theeadSize);
        final CountDownLatch countDownLatch = new CountDownLatch(userMonitorDtos.size());
        for (UserMonitorDto user : userMonitorDtos) {
            executor.execute(new TaskHTML(user, countDownLatch, url, atomicLoop, bak_atomicLoop));
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

    /**
     * 先请求html，再请求app
     */
    @SneakyThrows
    public void moreRedis(AtomicInteger atomicLoop, AtomicInteger bak_atomicLoop, Integer theeadSize) {

        List<UserMonitorDto> userMonitorDtos =
            userMonitorRepository.listForPage(null, null, 999999999, UserMonitorDto.class);
        log.info("多线程数量：{}，当前size：{}", theeadSize, userMonitorDtos.size());

        long start = System.currentTimeMillis();
        ExecutorService executor = ThreadUtil.newExecutor(theeadSize);
        final CountDownLatch countDownLatch = new CountDownLatch(userMonitorDtos.size());
        for (UserMonitorDto user : userMonitorDtos) {
            executor.execute(new MoreRedis(user, countDownLatch, atomicLoop, bak_atomicLoop));
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
                                .uid(user.getUid())
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

    @SuppressWarnings("all")
    public void sendRequestHTML(UserMonitorDto user, String url, AtomicInteger atomicLoop,
        AtomicInteger bak_atomicLoop) {

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
                                .uid(user.getUid())
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

                        if (redisUtils.hasKey(map1.getAweme_id()
                            .toString())) {
                            RedisDto redisDto = (RedisDto)redisUtils.get(map1.getAweme_id()
                                .toString());
                            awemeDtoBuilder.with_goods(true)
                                .product_url_list(redisDto.getUrl_list())
                                .extra(redisDto.getExtra());

                            log.info("当前awemeid存在redis={}，value={}", map1.getAweme_id()
                                .toString(), redisDto);
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
     * 将挂车视频放到redis缓存
     */
    @SuppressWarnings("all")
    public void sendRequestOnlyUpdateGoods(UserMonitorDto user, AtomicInteger atomicLoop,
        AtomicInteger atomicLoop_bak) {

        String result = StringUtils.EMPTY;
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
                .postForObject("http://127.0.0.1:9822/api/aweme/appUserVideoList", requestEntity, String.class);

        } catch (Exception e) {
            log.error("异常{}个用户，uid：{}，sec_user_id：{}{}", incrementAndGet, user.getUid(), user.getSec_user_id(), e);
        }

        // 处理返回
        AwemeDto awemeDto = JSON.parseObject(result, AwemeDto.class);

        if (null == awemeDto) {
            return;
        }

        for (AwemeDataDto datum : awemeDto.getData()) {

            for (AwemeListObjectDto map1 : datum.getAweme_list()) {

                // 商品信息
                if (null != map1.getStatus() && null != map1.getStatus()
                    .getWith_goods() && map1.getStatus()
                    .getWith_goods()) {

                    try {
                        // 保存到redis
                        RedisDto build = RedisDto.builder()
                            .url_list(map1.getAnchor_info()
                                .getIcon()
                                .getUrl_list())
                            .extra(JSON.parseArray(map1.getAnchor_info()
                                .getExtra(), AwemeExtraDto.class))
                            .build();

                        //todo 有效期1小时
                        redisUtils.set(map1.getAweme_id()
                            .toString(), build, 3600);
                    } catch (Exception e) {
                        redisUtils.del(map1.getAweme_id()
                            .toString());
                        log.error("入库redis异常--删除已存在的视频--可能是掉车了：{},{},{}", map1.getAweme_id(), JSON.toJSONString(map1),
                            e);
                    }
                }
            }
        }
    }

    /**
     * 删除索引任务
     */
    public void deleteIndex(String days) {
        String s = DateUtil.formatDate(DateUtil.addDays(DateUtil.getCurrentDate(), Integer.parseInt(days)));
        String s1 = "dm_aweme_" + s;
        String s21 = "dm_aweme_result_" + s + "_2";
        String s22 = "dm_aweme_result_" + s + "_4";
        String s23 = "dm_aweme_result_" + s + "_6";
        String s24 = "dm_aweme_result_" + s + "_12";
        String s25 = "dm_aweme_result_" + s + "_24";

        log.info("定时任务，删除索引：{}", s1);
        log.info("定时任务，删除索引：{}", s21);
        log.info("定时任务，删除索引：{}", s22);
        log.info("定时任务，删除索引：{}", s23);
        log.info("定时任务，删除索引：{}", s24);
        log.info("定时任务，删除索引：{}", s25);

        awemeRepository.deleteIndex(s1);
        awemeRepository.deleteIndex(s21);
        awemeRepository.deleteIndex(s22);
        awemeRepository.deleteIndex(s23);
        awemeRepository.deleteIndex(s24);
        awemeRepository.deleteIndex(s25);
    }

    class Task implements Runnable {

        private UserMonitorDto user;
        private CountDownLatch countDownLatch;
        private String url;
        private AtomicInteger atomicLoop;
        private AtomicInteger bak_atomicLoop;

        public Task(UserMonitorDto user, CountDownLatch countDownLatch, String url, AtomicInteger atomicLoop,
            AtomicInteger bak_atomicLoop) {
            this.user = user;
            this.countDownLatch = countDownLatch;
            this.url = url;
            this.atomicLoop = atomicLoop;
            this.bak_atomicLoop = bak_atomicLoop;
        }

        @Override
        public void run() {
            try {
                sendRequest(user, url, atomicLoop, bak_atomicLoop);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    class TaskHTML implements Runnable {

        private UserMonitorDto user;
        private CountDownLatch countDownLatch;
        private String url;
        private AtomicInteger atomicLoop;
        private AtomicInteger bak_atomicLoop;

        public TaskHTML(UserMonitorDto user, CountDownLatch countDownLatch, String url, AtomicInteger atomicLoop,
            AtomicInteger bak_atomicLoop) {
            this.user = user;
            this.countDownLatch = countDownLatch;
            this.url = url;
            this.atomicLoop = atomicLoop;
            this.bak_atomicLoop = bak_atomicLoop;
        }

        @Override
        public void run() {
            try {
                sendRequestHTML(user, url, atomicLoop, bak_atomicLoop);
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

    /**
     * 先请求html，再请求app
     */
    class TaskRequestHtmlApp implements Runnable {

        private UserMonitorDto user;
        private CountDownLatch countDownLatch;
        private String url;
        private AtomicInteger atomicLoop;
        private AtomicInteger bak_atomicLoop;

        public TaskRequestHtmlApp(UserMonitorDto user, CountDownLatch countDownLatch, String url,
            AtomicInteger atomicLoop, AtomicInteger bak_atomicLoop) {
            this.user = user;
            this.countDownLatch = countDownLatch;
            this.url = url;
            this.atomicLoop = atomicLoop;
            this.bak_atomicLoop = bak_atomicLoop;
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

    class MoreRedis implements Runnable {

        private UserMonitorDto user;
        private CountDownLatch countDownLatch;
        private AtomicInteger atomicLoop;
        private AtomicInteger bak_atomicLoop;

        public MoreRedis(UserMonitorDto user, CountDownLatch countDownLatch, AtomicInteger atomicLoop,
            AtomicInteger bak_atomicLoop) {
            this.user = user;
            this.countDownLatch = countDownLatch;
            this.atomicLoop = atomicLoop;
            this.bak_atomicLoop = bak_atomicLoop;
        }

        @Override
        public void run() {
            try {
                sendRequestOnlyUpdateGoods(user, atomicLoop, bak_atomicLoop);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        }
    }
}
