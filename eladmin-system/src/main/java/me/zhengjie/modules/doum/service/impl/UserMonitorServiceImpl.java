package me.zhengjie.modules.doum.service.impl;

import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import com.cdos.web.htppclient.CdosHttpRestTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import me.zhengjie.modules.doum.repository.UserMonitorRepository;
import me.zhengjie.modules.doum.service.UserMonitorService;
import me.zhengjie.modules.doum.service.dto.UserMonitorDto;
import me.zhengjie.modules.doum.service.dto.UserMonitorQueryCriteria;
import me.zhengjie.utils.PageUtil;
import me.zhengjie.utils.SecurityUtils;
import me.zhengjie.utils.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Set;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class UserMonitorServiceImpl implements UserMonitorService {
    @Autowired
    UserMonitorRepository userMonitorRepository;
    @Autowired
    CdosHttpRestTemplate userHttpRestTemplate;

    @Override
    public Object list(UserMonitorQueryCriteria criteria, Pageable pageable) {
        FieldSortBuilder sortBuilder = new FieldSortBuilder("create_time").order(SortOrder.DESC);

        CdosApiPageResponse<UserMonitorDto> awemeDtoCdosApiPageResponse =
            userMonitorRepository.listForPage(new PageInfo(pageable.getPageNumber() + 1, pageable.getPageSize()),
                queryBuilder(criteria), sortBuilder, UserMonitorDto.class);

        return PageUtil.toPage(awemeDtoCdosApiPageResponse.getResult(), awemeDtoCdosApiPageResponse.getPage()
            .getTotalCount());
    }

    @Override
    public Object add(UserMonitorDto dto) {
        userMonitorRepository.insert(dto);
        return Boolean.TRUE;
    }

    @Override
    public Object getUser(UserMonitorQueryCriteria criteria) {

        String[] split = criteria.getHome_url()
            .split("\n");
        for (String i : split) {

            String home_url = i.substring(i.indexOf("https"));
            String s = HttpUtil.get(home_url);
            int start = s.indexOf("user");
            int end = s.indexOf("?");
            String substring = s.substring(start, end);
            // 用户的sec_user_id信息
            String sec_user_id = substring.substring(5);

            ResponseEntity<String> entity = userHttpRestTemplate.getDelegate()
                .getForEntity("http://localhost:8868/getUser?sec_user_id=" + sec_user_id + "&current_user="
                    + SecurityUtils.getCurrentUserId() + "&home_url=" + home_url, String.class);
            log.info("runAweme 执行完毕 response : {}", JSON.toJSONString(entity));
        }
        return null;
    }

    @Override
    public Object delete(Set<String> ids) {
        for (String id : ids) {
            userMonitorRepository.delete(id);
        }
        return Boolean.TRUE;
    }

    public BoolQueryBuilder queryBuilder(UserMonitorQueryCriteria criteria) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (StringUtils.isNoneBlank(criteria.getNickname())) {
            boolQueryBuilder.must(QueryBuilders.termQuery("nickname", criteria.getNickname()));
        }

        log.info(boolQueryBuilder);
        return boolQueryBuilder;
    }
}
