package me.zhengjie.modules.doum.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import me.zhengjie.modules.doum.repository.AwemeRepository;
import me.zhengjie.modules.doum.repository.UserMonitorRepository;
import me.zhengjie.modules.doum.service.DashBoardService;
import me.zhengjie.modules.doum.service.dto.AwemeQueryCriteria;
import me.zhengjie.modules.doum.service.dto.DashBoardQueryCriteria;
import me.zhengjie.utils.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class DashBoardServiceImpl implements DashBoardService {
    @Autowired
    AwemeRepository awemeRepository;

    @Autowired
    UserMonitorRepository userMonitorRepository;

    @Override
    public Object statisticsUser(DashBoardQueryCriteria criteria) {
        return null;
    }

    public BoolQueryBuilder queryBuilder(AwemeQueryCriteria criteria) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (StringUtils.isNoneBlank(criteria.getNickname())) {
            boolQueryBuilder.must(QueryBuilders.termQuery("nickname", criteria.getNickname()));
        }

        log.info(boolQueryBuilder);
        return boolQueryBuilder;
    }
}
