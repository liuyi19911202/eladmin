package me.zhengjie.modules.doum.repository;

import com.alibaba.fastjson.JSON;
import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import com.cdos.es.estemplate.repository.BaseElasticSearchRepository;
import com.cdos.utils.DateUtil;
import com.cdos.utils.Safes;
import lombok.extern.log4j.Log4j2;
import me.zhengjie.domain.Log;
import me.zhengjie.modules.doum.enums.DateBetweenEnum;
import me.zhengjie.modules.doum.service.dto.AwemeDto;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@Log4j2
@Component
public class AwemeRepository extends BaseElasticSearchRepository<AwemeDto> {
    private String index = "dm_aweme";
    private String doc = "_doc";

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    public CdosApiPageResponse<AwemeDto> listForPage(PageInfo page, AbstractQueryBuilder boolQueryBuilder,
        SortBuilder sortBuilder, Class clazz) {
        return super.listForPage(index + "_" + DateUtil.formatDate(DateUtil.getCurrentDate()), doc, page,
            boolQueryBuilder, sortBuilder, clazz);
    }

    public void delete(QueryBuilder elasticsearchQuery) {
        super.delete(index + "_" + DateUtil.formatDate(DateUtil.getCurrentDate()), doc, elasticsearchQuery);
    }

    public List<AwemeDto> listForPage(String from, AbstractQueryBuilder boolQueryBuilder, SortBuilder sortBuilder,
        Integer n, Class clazz) {
        return super.listForPage(getIndexForDetail(from), doc, boolQueryBuilder, sortBuilder, n, clazz);
    }

    public List<AwemeDto> aggrList(String from, BoolQueryBuilder boolQueryBuilder, Integer size, SortOrder sortOrder) {
        FieldSortBuilder update_time = new FieldSortBuilder("update_time").order(sortOrder);

        Aggregations aggregations =
            getAggregations(getIndex(from), doc, size, boolQueryBuilder, update_time, "aweme_id", "update_time",
                sortOrder);
        Terms longTerms = aggregations.get("aweme_idGroup");
        List<? extends Terms.Bucket> buckets = longTerms.getBuckets();

        List<AwemeDto> update_timeTop = Safes.of(buckets)
            .parallelStream()
            .flatMap(map -> {

                // 内层循环
                InternalTopHits internalTopHits = map.getAggregations()
                    .get("update_timeTop");

                SearchHit[] hits = internalTopHits.getHits()
                    .getHits();

                List<AwemeDto> collect = Arrays.stream(hits)
                    .map(m -> JSON.parseObject(m.getSourceAsString(), AwemeDto.class))
                    .collect(Collectors.toList());

                return collect.stream();
            })
            .collect(Collectors.toList());

        return update_timeTop;
    }

    private Aggregations getAggregations(String[] index, String type, Integer n, BoolQueryBuilder boolQueryBuilder,
        SortBuilder sortBuilder, String group, String order, SortOrder sortOrder) {

        log.info("getAggregations boolQueryBuilder = {} ", boolQueryBuilder);

        // TODO: 2022/3/23 过滤掉为digg 0的
        boolQueryBuilder.mustNot(QueryBuilders.termQuery("digg_count", 0));

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(group + "Group")
            .field(group)
            .size(n);

        // 这样肯定是1，因为倒叙、正序只取第一位
        termsAggregationBuilder.subAggregation(AggregationBuilders.topHits(order + "Top")
            .sort("update_time", sortOrder)
            .size(1));

        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder().withIndices(index)
            .withTypes(type)
            .addAggregation(termsAggregationBuilder);

        if (Objects.nonNull(boolQueryBuilder)) {
            nativeSearchQueryBuilder.withQuery(boolQueryBuilder);
        }

        // 判断是否加上排序
        if (Objects.nonNull(sortBuilder)) {
            nativeSearchQueryBuilder.withSort(sortBuilder);
        }

        Aggregations aggregations =
            elasticsearchTemplate.query(nativeSearchQueryBuilder.build(), new ResultsExtractor<Aggregations>() {
                @Override
                public Aggregations extract(SearchResponse searchResponse) {
                    return searchResponse.getAggregations();
                }
            });
        return aggregations;
    }

    public void insert(List<AwemeDto> list, String index) {
        super.insert(index, doc, list);
    }

    public void insert(AwemeDto dto, String index) {
        super.insert(index, doc, dto);
    }

    /**
     * 主要判断开始时间，决定是否跨索引
     */
    private static String[] getIndex(String from) {

        /**
         * 当日的00：00：00
         */
        Date currentDate = DateUtil.getCurrentDate();

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
            // 取指定索引
            String[] strings = {"dm_aweme" + "_" + DateUtil.formatDate(currentDate),
                "dm_aweme" + "_" + DateUtil.formatDate(startDate)};

            log.info("取指定的索引：{}", Arrays.toString(strings));
            return strings;
        }

        return new String[] {"dm_aweme" + "_" + DateUtil.formatDate(currentDate)};
    }

    private static String[] getIndexForDetail(String from) {

        /**
         * 当日的00：00：00
         */
        Date currentDate = DateUtil.getCurrentDate();
        Date startDate = DateUtil.parseDate(from);

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

            List<Date> dates = DateUtil.betweenDays(startDate, currentDate);
            List<String> collect = Safes.of(dates)
                .stream()
                .map(DateUtil::formatDate)
                .map(map -> "dm_aweme" + "_" + map)
                .collect(Collectors.toList());
            String[] strings = new String[collect.size()];
            // 取指定索引
            log.info("取指定的索引：{}", Arrays.toString(collect.toArray(strings)));

            return collect.toArray(strings);
        }

        return new String[] {"dm_aweme" + "_" + DateUtil.formatDate(currentDate)};
    }

}
