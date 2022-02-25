package me.zhengjie.modules.doum.repository;

import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import com.cdos.es.estemplate.repository.BaseElasticSearchRepository;
import me.zhengjie.modules.doum.service.dto.AwemeDto;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.stereotype.Component;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@Component
public class AwemeRepository extends BaseElasticSearchRepository<AwemeDto> {
    final private String index = "dm_aweme";
    final private String doc = "_doc";

    public CdosApiPageResponse<AwemeDto> listForPage(PageInfo page, AbstractQueryBuilder boolQueryBuilder,
        SortBuilder sortBuilder, Class clazz) {
        return super.listForPage(index, doc, page, boolQueryBuilder, sortBuilder, clazz);
    }
}
