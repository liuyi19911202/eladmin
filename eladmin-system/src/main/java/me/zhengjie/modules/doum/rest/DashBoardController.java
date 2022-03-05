package me.zhengjie.modules.doum.rest;

/**
 * @author liuyi
 * @date 2022/2/25
 */

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import me.zhengjie.annotation.AnonymousAccess;
import me.zhengjie.annotation.Log;
import me.zhengjie.modules.doum.service.DashBoardService;
import me.zhengjie.modules.doum.service.dto.DashBoardQueryCriteria;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 仪表盘
 *
 * @author liuyi
 */
@Api(tags = "抖音：仪表盘")
@RestController
@RequestMapping("/api/dashboard")
@RequiredArgsConstructor
public class DashBoardController {

    DashBoardService dashBoardService;

    @Log("统计用户信息")
    @ApiOperation("统计用户信息")
    @GetMapping("/statisticsUser")
    @AnonymousAccess
    public ResponseEntity<Object> statisticsUser(DashBoardQueryCriteria criteria) {
        return new ResponseEntity<>(dashBoardService.statisticsUser(criteria), HttpStatus.OK);
    }
}
