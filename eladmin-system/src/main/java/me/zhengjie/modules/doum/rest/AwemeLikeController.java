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
import me.zhengjie.annotation.Log;
import me.zhengjie.modules.doum.service.AwemeLikeService;
import me.zhengjie.modules.doum.service.dto.AwemeLikeQueryCriteria;
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
@Api(tags = "抖音：作品点赞趋势")
@RestController
@RequestMapping("/api/aweme/like")
@RequiredArgsConstructor
public class AwemeLikeController {

    private final AwemeLikeService awemeLikeService;

    @Log("作品点赞趋势列表")
    @ApiOperation("作品点赞趋势列表查询")
    @GetMapping
    @PreAuthorize("@el.check('aweme:like:list')")
    public ResponseEntity<Object> queryAweme(AwemeLikeQueryCriteria criteria, Pageable pageable) {
        return new ResponseEntity<>(awemeLikeService.list(criteria, pageable), HttpStatus.OK);
    }

    @Log("作品点赞趋势详情")
    @ApiOperation("作品点赞趋势详情")
    @GetMapping("/queryAwemeDetail")
    @PreAuthorize("@el.check('aweme:like:detail')")
    public ResponseEntity<Object> queryAwemeDetail(AwemeLikeQueryCriteria criteria) {
        return new ResponseEntity<>(awemeLikeService.detail(criteria), HttpStatus.OK);
    }
}
