package me.zhengjie.modules.doum.repository;

import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import com.cdos.es.estemplate.repository.BaseElasticSearchRepository;
import com.cdos.utils.DateUtil;
import lombok.extern.log4j.Log4j2;
import me.zhengjie.domain.Log;
import me.zhengjie.modules.doum.enums.DateBetweenEnum;
import me.zhengjie.modules.doum.service.dto.AwemeDto;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@Log4j2
@Component
public class AwemeRepository extends BaseElasticSearchRepository<AwemeDto> {
    private String date = DateUtil.formatDate(DateUtil.getCurrentDate());
    private String index = "dm_aweme" + "_" + date;
    private String doc = "_doc";

    public CdosApiPageResponse<AwemeDto> listForPage(PageInfo page, AbstractQueryBuilder boolQueryBuilder,
        SortBuilder sortBuilder, Class clazz) {
        return super.listForPage(index, doc, page, boolQueryBuilder, sortBuilder, clazz);
    }

    public List<AwemeDto> listForPage(String from, AbstractQueryBuilder boolQueryBuilder, SortBuilder sortBuilder,
        Integer n, Class clazz) {
        return super.listForPage(getIndex(from), doc, boolQueryBuilder, sortBuilder, n, clazz);
    }

    public List<Pair<String, List<Pair<String, Long>>>> aggregationLongFieldTopN(Integer n,
        AbstractQueryBuilder boolQueryBuilder, SortBuilder sortBuilder, String... field) {
        return super.aggregationLongFieldTopN(index, doc, n, boolQueryBuilder, sortBuilder, field);
    }

    public void insert(List<AwemeDto> list, String index) {
        super.insert(index, doc, list);
    }

    /**
     * 主要判断开始时间，决定是否跨索引
     */
    private static String[] getIndex(String from) {

        /**
         * 当日的00：00：00
         */
        Date currentDate = DateUtil.getCurrentDate();

        DateUtil.formatDate(DateUtil.getDate(), "yyyy-MM-dd HH:mm:ss");
        Date startDate = DateUtil.parseDatetime(from);

        /**
         * 1 currentDate > date
         * -1 currentDate < date
         * 0 currentDate =  date
         */
        if (DateUtil.comapreDateTime(startDate, currentDate) == 1
            || DateUtil.comapreDateTime(startDate, currentDate) == 0) {
            // 还是取当天的索引
            log.info("取当天的索引：{}", new String[] {"dm_aweme" + "_" + DateUtil.formatDate(currentDate)});

            return new String[] {"dm_aweme" + "_" + DateUtil.formatDate(currentDate)};
        } else if (DateUtil.comapreDateTime(startDate, currentDate) == -1) {
            // 取全部索引
            log.info("取指定的索引：{}，{}", new String[] {"dm_aweme" + "_" + DateUtil.formatDate(currentDate),
                "dm_aweme" + "_" + DateUtil.formatDate(startDate)});

            return new String[] {"dm_aweme" + "_" + DateUtil.formatDate(currentDate),
                "dm_aweme" + "_" + DateUtil.formatDate(startDate)};
        }

        return new String[] {"dm_aweme" + "_" + DateUtil.formatDate(currentDate)};
    }

}
