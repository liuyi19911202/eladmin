package me.zhengjie.modules.doum.service;

import me.zhengjie.modules.doum.service.dto.DashBoardQueryCriteria;

public interface DashBoardService {

    Object statisticsUser(DashBoardQueryCriteria criteria);
}
