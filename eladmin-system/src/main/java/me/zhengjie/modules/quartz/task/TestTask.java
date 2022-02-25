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

import com.alibaba.fastjson.JSON;
import com.cdos.web.htppclient.CdosHttpRestTemplate;
import lombok.extern.slf4j.Slf4j;
import me.zhengjie.modules.doum.repository.UserMonitorRepository;
import me.zhengjie.modules.doum.service.dto.UserMonitorDto;
import me.zhengjie.utils.SecurityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;

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
                        .getCurrentUser(), String.class);

                log.info("runAweme 执行完毕 response : {}", JSON.toJSONString(entity));
            } catch (Exception e) {
                log.info("当前执行到第{}/{}个用户，uid：{}，sec_user_id：{}", userMonitorDtos.size(), i, userMonitorDtos.get(i)
                    .getUid(), userMonitorDtos.get(i)
                    .getSec_user_id());
            }
        }
    }
}
