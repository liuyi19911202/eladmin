package me.zhengjie.modules.doum.repository;

import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import com.cdos.es.estemplate.repository.BaseElasticSearchRepository;
import com.cdos.utils.DateUtil;
import lombok.extern.log4j.Log4j2;
import me.zhengjie.modules.doum.enums.DateBetweenEnum;
import me.zhengjie.modules.doum.service.dto.AwemeDto;
import me.zhengjie.modules.doum.service.dto.AwemeResultDto;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@Log4j2
@Component
public class AwemeResult2Repository extends BaseElasticSearchRepository<AwemeResultDto> {
    private String index = "dm_aweme_result";
    private String doc = "_doc";

    public CdosApiPageResponse<AwemeResultDto> listForPage(PageInfo page, AbstractQueryBuilder boolQueryBuilder,
        SortBuilder sortBuilder, Class clazz) {
        return super.listForPage(index + "_" + DateUtil.formatDate(DateUtil.getCurrentDate()) + "_2", doc, page,
            boolQueryBuilder, sortBuilder, clazz);
    }

    public List<AwemeResultDto> listForPage(DateBetweenEnum dateBetweenEnum, AbstractQueryBuilder boolQueryBuilder,
        SortBuilder sortBuilder, Integer n, Class clazz) {
        return super.listForPage(index + "_" + DateUtil.formatDate(DateUtil.getCurrentDate()) + "_2", doc,
            boolQueryBuilder, sortBuilder, n, clazz);
    }

    public Boolean deleteIndex() {
        return super.deleteIndex(index + "_" + DateUtil.formatDate(DateUtil.getCurrentDate()) + "_2");
    }

    public void insert(List<AwemeResultDto> list, String index) {
        super.insert(index, doc, list);
    }

}
