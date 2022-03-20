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
import me.zhengjie.modules.doum.service.AwemeService;
import me.zhengjie.modules.doum.service.dto.AwemeQueryCriteria;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 抖音作品相关
 */
@Api(tags = "抖音：作品")
@RestController
@RequestMapping("/api/aweme")
@RequiredArgsConstructor
public class AwemeController {

    private final AwemeService awemeService;

    @Log("列表查询抖音作品列表")
    @ApiOperation("抖音作品列表")
    @GetMapping
    @AnonymousAccess
    public ResponseEntity<Object> queryAweme(AwemeQueryCriteria criteria, Pageable pageable) {
        return new ResponseEntity<>(awemeService.list(criteria, pageable), HttpStatus.OK);
    }

    @Log("抖音爬虫最新列表")
    @ApiOperation("抖音作品最新列表")
    @GetMapping("/queryNewAweme")
    @AnonymousAccess
    public ResponseEntity<Object> queryNewAweme(AwemeQueryCriteria criteria, Pageable pageable) {
        return new ResponseEntity<>(awemeService.newList(criteria, pageable), HttpStatus.OK);
    }
}
