package me.zhengjie.modules.doum.repository;

import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import com.cdos.es.estemplate.repository.BaseElasticSearchRepository;
import me.zhengjie.modules.doum.repository.dto.whitelist.AwemeWhitelistDto;
import me.zhengjie.modules.doum.service.dto.UserRemarkDto;
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
public class AwemeWhitelistRepository extends BaseElasticSearchRepository<AwemeWhitelistDto> {
    final private String index = "dm_aweme_whitelist";
    final private String doc = "_doc";

    public void insert(AwemeWhitelistDto dto) {
        super.insert(index, doc, dto);
    }

    public void delete(String id) {
        super.delete(index, doc, id);
    }

    public void delete(QueryBuilder elasticsearchQuery) {
        super.delete(index, doc, elasticsearchQuery);
    }

    public CdosApiPageResponse<AwemeWhitelistDto> listForPage(PageInfo page, AbstractQueryBuilder boolQueryBuilder,
        SortBuilder sortBuilder, Class clazz) {
        return super.listForPage(index, doc, page, boolQueryBuilder, sortBuilder, clazz);
    }

    public List<AwemeWhitelistDto> listForPage(AbstractQueryBuilder boolQueryBuilder, SortBuilder sortBuilder,
        Integer n, Class clazz) {
        return super.listForPage(index, doc, boolQueryBuilder, sortBuilder, n, clazz);
    }

}
