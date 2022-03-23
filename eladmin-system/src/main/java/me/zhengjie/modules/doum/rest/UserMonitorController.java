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
package me.zhengjie.modules.doum.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import me.zhengjie.annotation.AnonymousAccess;
import me.zhengjie.annotation.Log;
import me.zhengjie.modules.doum.service.UserMonitorService;
import me.zhengjie.modules.doum.service.dto.UserMonitorDto;
import me.zhengjie.modules.doum.service.dto.UserMonitorQueryCriteria;
import me.zhengjie.modules.system.domain.User;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.Set;

/**
 * @author Zheng Jie
 * @date 2018-11-23
 */
@Api(tags = "抖音：用户监控")
@RestController
@RequestMapping("/api/doum/users")
@RequiredArgsConstructor
public class UserMonitorController {

    private final UserMonitorService userMonitorService;

    @ApiOperation("查询用户监控列表")
    @GetMapping
    @AnonymousAccess
    public ResponseEntity<Object> queryUser(UserMonitorQueryCriteria criteria, Pageable pageable) {

        return new ResponseEntity<>(userMonitorService.list(criteria, pageable), HttpStatus.OK);
    }

    @Log("爬虫用户信息")
    @ApiOperation("爬虫用户信息")
    @PostMapping("/getUser")
    @AnonymousAccess
    public ResponseEntity<Object> getUser(UserMonitorQueryCriteria criteria) {
        userMonitorService.getUser1(criteria);
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @Log("爬虫用户信息")
    @ApiOperation("爬虫用户信息")
    @PostMapping
    @AnonymousAccess
    public ResponseEntity<Object> add(@RequestBody UserMonitorQueryCriteria criteria) {
        userMonitorService.getUser1(criteria);
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @Log("获取用户备注")
    @ApiOperation("获取用户备注")
    @GetMapping("/updateRemark")
    @AnonymousAccess
    public ResponseEntity<Object> getRemark(UserMonitorQueryCriteria criteria) {
        return new ResponseEntity<>(userMonitorService.getRemark(criteria), HttpStatus.OK);
    }

    @Log("修改备注")
    @ApiOperation("修改用户备注")
    @PostMapping("/updateRemark")
    @AnonymousAccess
    public ResponseEntity<Object> updateRemark(UserMonitorQueryCriteria criteria) {
        userMonitorService.updateRemark(criteria);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @Log("删除监控用户")
    @ApiOperation("删除监控用户")
    @DeleteMapping
    @AnonymousAccess
    public ResponseEntity<Object> deleteUser(@RequestBody Set<Long> ids) {
        userMonitorService.delete(ids);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
