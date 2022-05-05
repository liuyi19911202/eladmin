package me.zhengjie.modules.doum.service.impl;

import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import com.cdos.utils.DateUtil;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import me.zhengjie.modules.doum.repository.AwemeRepository;
import me.zhengjie.modules.doum.service.AwemeService;
import me.zhengjie.modules.doum.service.UserMonitorService;
import me.zhengjie.modules.doum.service.dto.AwemeDto;
import me.zhengjie.modules.doum.service.dto.AwemeQueryCriteria;
import me.zhengjie.utils.PageUtil;
import me.zhengjie.utils.SecurityUtils;
import me.zhengjie.utils.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@Service
@RequiredArgsConstructor
@Log4j2
@CacheConfig(cacheNames = "awemelist")
public class AwemeServiceImpl implements AwemeService {
    @Autowired
    AwemeRepository awemeRepository;
    @Autowired
    UserMonitorService userMonitorService;

    /**
     * 取最新的一批次爬虫数据
     *
     * @param criteria
     * @param pageable
     * @return
     */
    @Override
    public Object newList(AwemeQueryCriteria criteria, Pageable pageable) {
        FieldSortBuilder sortBuilder = new FieldSortBuilder("update_time").order(SortOrder.DESC);

        CdosApiPageResponse<AwemeDto> awemeDtoCdosApiPageResponse =
            awemeRepository.listForPage(new PageInfo(pageable.getPageNumber() + 1, pageable.getPageSize()),
                queryBuilder(Pair.of(com.cdos.utils.DateUtil.formatDate(
                    com.cdos.utils.DateUtil.addHours(com.cdos.utils.DateUtil.getCurrentDateTime(), -2),
                    com.cdos.utils.DateUtil.FORMAT_DATE_TIME),
                    com.cdos.utils.DateUtil.formatDate(com.cdos.utils.DateUtil.getCurrentDateTime(),
                        DateUtil.FORMAT_DATE_TIME)), criteria), sortBuilder, AwemeDto.class);

        if (null != awemeDtoCdosApiPageResponse.getResult()) {

            // 先获取最大的时间
            Date date = awemeDtoCdosApiPageResponse.getResult()
                .stream()
                .max(Comparator.comparing(AwemeDto::getUpdate_time))
                .map(AwemeDto::getUpdate_time)
                .orElse(null);

            if (date != null) {
                // 取2个小时内的数据就够了
                Map<Date, List<AwemeDto>> collect = awemeDtoCdosApiPageResponse.getResult()
                    .stream()
                    .collect(Collectors.groupingBy(AwemeDto::getUpdate_time));

                if (collect.containsKey(date)) {
                    List<AwemeDto> awemeDtos = collect.get(date);
                    // 按发布时间排序
                    List<AwemeDto> collect1 = awemeDtos.stream()
                        .map(map -> {
                            map.setStr_aweme_id(String.valueOf(map.getAweme_id()));
                            return map;
                        })
                        .sorted(Comparator.comparing(AwemeDto::getCreate_time)
                            .reversed())
                        .collect(Collectors.toList());
                    // 排序
                    return PageUtil.toPage(collect1, collect1.size());
                } else {
                    log.error("有最大时间，列表中未匹配到，异常！{}", awemeDtoCdosApiPageResponse);
                }
            } else {
                log.error("未查询到最新的数据记录:{}", awemeDtoCdosApiPageResponse);
            }

        }

        return PageUtil.toPage(Lists.newArrayListWithExpectedSize(0), 0);
    }

    @Override
    public Object list(AwemeQueryCriteria criteria, Pageable pageable) {
        FieldSortBuilder sortBuilder = new FieldSortBuilder("update_time").order(SortOrder.DESC);

        CdosApiPageResponse<AwemeDto> awemeDtoCdosApiPageResponse =
            awemeRepository.listForPage(new PageInfo(pageable.getPageNumber() + 1, pageable.getPageSize()),
                queryBuilder(criteria), sortBuilder, AwemeDto.class);

        return PageUtil.toPage(awemeDtoCdosApiPageResponse.getResult(), awemeDtoCdosApiPageResponse.getPage()
            .getTotalCount());
    }

    public BoolQueryBuilder queryBuilder(Pair<String, String> date, AwemeQueryCriteria criteria) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        boolQueryBuilder.filter(QueryBuilders.rangeQuery("update_time")
            .gte(date.getLeft())
            .lte(date.getRight()));

        boolQueryBuilder.must(QueryBuilders.termQuery("uid", criteria.getUid()));

        if (SecurityUtils.getCurrentUserId() != 1) {
            boolQueryBuilder.must(QueryBuilders.termsQuery("author_user_id", userMonitorService.getAuthorUserId()));
        }

        log.info(boolQueryBuilder);
        return boolQueryBuilder;
    }

    public BoolQueryBuilder queryBuilder(AwemeQueryCriteria criteria) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (SecurityUtils.getCurrentUserId() != 1) {
            boolQueryBuilder.must(QueryBuilders.termsQuery("author_user_id", userMonitorService.getAuthorUserId()));
        }

        if (StringUtils.isNoneBlank(criteria.getNickname())) {
            boolQueryBuilder.must(QueryBuilders.termQuery("nickname", criteria.getNickname()));
        }

        if (Objects.nonNull(criteria.getUnique_id())) {
            boolQueryBuilder.must(QueryBuilders.termQuery("unique_id", criteria.getUnique_id()));
        }

        log.info(boolQueryBuilder);
        return boolQueryBuilder;
    }
}
