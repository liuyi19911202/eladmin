package me.zhengjie.modules.doum.repository;

import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import com.cdos.es.estemplate.repository.BaseElasticSearchRepository;
import me.zhengjie.modules.doum.service.dto.AwemeDto;
import me.zhengjie.modules.doum.service.dto.UserMonitorDto;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@Component
public class UserMonitorRepository extends BaseElasticSearchRepository<UserMonitorDto> {
    final private String index = "dm_user_monitor";
    final private String doc = "_doc";

    public void insert(UserMonitorDto dto) {
        super.insert(index, doc, dto);
    }

    public void delete(String id) {
        super.delete(index, doc, id);
    }

    public CdosApiPageResponse<UserMonitorDto> listForPage(PageInfo page, AbstractQueryBuilder boolQueryBuilder,
        SortBuilder sortBuilder, Class clazz) {
        return super.listForPage(index, doc, page, boolQueryBuilder, sortBuilder, clazz);
    }

    public List<UserMonitorDto> listForPage(AbstractQueryBuilder boolQueryBuilder, SortBuilder sortBuilder, Integer n,
        Class clazz) {
        return super.listForPage(index, doc, boolQueryBuilder, sortBuilder, n, clazz);
    }
}
