package me.zhengjie.modules.doum.service.impl;

import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import me.zhengjie.modules.doum.repository.AwemeRepository;
import me.zhengjie.modules.doum.service.AwemeService;
import me.zhengjie.modules.doum.service.dto.AwemeDto;
import me.zhengjie.modules.doum.service.dto.AwemeQueryCriteria;
import me.zhengjie.utils.DateUtil;
import me.zhengjie.utils.PageUtil;
import me.zhengjie.utils.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
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
public class AwemeServiceImpl implements AwemeService {
    @Autowired
    AwemeRepository awemeRepository;

    public static void main(String[] args) {
        System.out.println(DateUtil.fromTimeStamp(1645445162L));
    }

    @Override
    public Object list(AwemeQueryCriteria criteria, Pageable pageable) {
        FieldSortBuilder sortBuilder = new FieldSortBuilder("create_time").order(SortOrder.DESC);

        CdosApiPageResponse<AwemeDto> awemeDtoCdosApiPageResponse =
            awemeRepository.listForPage(new PageInfo(pageable.getPageNumber() + 1, pageable.getPageSize()),
                queryBuilder(criteria), sortBuilder, AwemeDto.class);

        return PageUtil.toPage(awemeDtoCdosApiPageResponse.getResult(), awemeDtoCdosApiPageResponse.getPage()
            .getTotalCount());
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
