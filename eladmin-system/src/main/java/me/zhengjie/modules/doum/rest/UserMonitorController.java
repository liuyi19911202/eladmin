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
    @PreAuthorize("@el.check('doum:user:list')")
    public ResponseEntity<Object> queryUser(UserMonitorQueryCriteria criteria, Pageable pageable) {

        return new ResponseEntity<>(userMonitorService.list(criteria, pageable), HttpStatus.OK);
    }

    @Log("爬虫用户信息")
    @ApiOperation("爬虫用户信息")
    @GetMapping("/getUser")
    @AnonymousAccess
    public ResponseEntity<Object> getUser(UserMonitorQueryCriteria criteria) {
        userMonitorService.getUser(criteria);
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @Log("新增监控用户")
    @ApiOperation("新增监控用户")
    @PostMapping
    @PreAuthorize("@el.check('doum:user:add')")
    public ResponseEntity<Object> createUser(@Validated @RequestBody UserMonitorDto resources) {
        //        userMonitorService.add(resources);
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @Log("删除监控用户")
    @ApiOperation("删除监控用户")
    @DeleteMapping
    @PreAuthorize("@el.check('doum:user:del')")
    public ResponseEntity<Object> deleteUser(@RequestBody Set<String> ids) {
        userMonitorService.delete(ids);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
