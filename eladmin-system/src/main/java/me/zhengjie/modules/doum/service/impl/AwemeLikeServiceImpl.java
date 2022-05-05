package me.zhengjie.modules.doum.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import com.alibaba.fastjson.JSON;
import com.alipay.api.domain.Person;
import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import com.cdos.utils.DateUtil;
import com.cdos.utils.Safes;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import me.zhengjie.modules.doum.enums.DateBetweenEnum;
import me.zhengjie.modules.doum.repository.*;
import me.zhengjie.modules.doum.repository.dto.whitelist.AwemeWhitelistDto;
import me.zhengjie.modules.doum.service.AwemeLikeService;
import me.zhengjie.modules.doum.service.UserMonitorService;
import me.zhengjie.modules.doum.service.dto.*;
import me.zhengjie.utils.PageUtil;
import me.zhengjie.utils.SecurityUtils;
import me.zhengjie.utils.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.poi.ss.formula.functions.T;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
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
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@Service
@CacheConfig(cacheNames = "awemelike")
@RequiredArgsConstructor
@Log4j2
public class AwemeLikeServiceImpl implements AwemeLikeService {
    @Autowired
    AwemeRepository awemeRepository;
    @Autowired
    AwemeResult2Repository awemeResult2Repository;
    @Autowired
    AwemeResult4Repository awemeResult4Repository;
    @Autowired
    AwemeResult6Repository awemeResult6Repository;
    @Autowired
    AwemeResult12Repository awemeResult12Repository;
    @Autowired
    AwemeResult24Repository awemeResult24Repository;
    @Autowired
    UserMonitorService userMonitorService;
    @Autowired
    UserRemarkRepository userRemarkRepository;
    @Autowired
    UserMonitorServiceImpl userMonitorServiceImpl;
    @Autowired
    AwemeWhitelistRepository awemeWhitelistRepository;
    @Autowired
    UserMonitorRepository userMonitorRepository;

    @Override
    public Object list(AwemeLikeQueryCriteria criteria, Pageable pageable) {
        // 按点赞diff排序
        FieldSortBuilder sortBuilder = new FieldSortBuilder("diff").order(SortOrder.DESC);
        DateBetweenEnum of = Safes.of(criteria.getDateBetweenEnum(), DateBetweenEnum.TWO_HOUR);
        CdosApiPageResponse<AwemeResultDto> responseList;
        switch (of) {
            case TWO_HOUR:
                responseList = awemeResult2Repository.listForPage(
                    new PageInfo(pageable.getPageNumber() + 1, pageable.getPageSize()), queryBuilder(criteria),
                    sortBuilder, AwemeResultDto.class);
                break;
            case FOUR_HOUR:
                responseList = awemeResult4Repository.listForPage(
                    new PageInfo(pageable.getPageNumber() + 1, pageable.getPageSize()), queryBuilder(criteria),
                    sortBuilder, AwemeResultDto.class);
                break;
            case SIX_HOUR:
                responseList = awemeResult6Repository.listForPage(
                    new PageInfo(pageable.getPageNumber() + 1, pageable.getPageSize()), queryBuilder(criteria),
                    sortBuilder, AwemeResultDto.class);
                break;
            case TWELVE_HOUR:
                responseList = awemeResult12Repository.listForPage(
                    new PageInfo(pageable.getPageNumber() + 1, pageable.getPageSize()), queryBuilder(criteria),
                    sortBuilder, AwemeResultDto.class);
                break;
            case ONE_DAY:
                responseList = awemeResult24Repository.listForPage(
                    new PageInfo(pageable.getPageNumber() + 1, pageable.getPageSize()), queryBuilder(criteria),
                    sortBuilder, AwemeResultDto.class);
                break;
            default:
                return new CdosApiPageResponse<AwemeResultDto>();
        }
        Long currentUserId = SecurityUtils.getCurrentUserId();
        return PageUtil.toPage(responseList.getResult()
            .parallelStream()
            .map(map -> {
                map.setStr_aweme_id(String.valueOf(map.getAweme_id()));
                UserRemarkDto remarkByUid =
                    userMonitorServiceImpl.getRemarkByUid(map.getAuthor_user_id(), currentUserId);
                if (null != remarkByUid) {
                    map.setRemark(remarkByUid.getRemark());
                }
                return map;
            })
            .filter(f -> {
                if (StringUtils.isNotBlank(criteria.getRemark())) {
                    return Safes.ofEmpty(f.getRemark(), "")
                        .contains(criteria.getRemark());
                }
                return true;
            })
            .collect(Collectors.toList()), responseList.getPage()
            .getTotalCount());
    }

    @Override
    public Object detail(AwemeLikeQueryCriteria criteria) {

        DateBetweenEnum of = Safes.of(criteria.getDateBetweenEnum(), DateBetweenEnum.THREE_DAY);
        Pair<String, String> dateBetween = switchDateBetweenDetail(of);

        FieldSortBuilder update_time = new FieldSortBuilder("update_time").order(SortOrder.ASC);

        // TODO: 2022/3/10 按照作品查询，10分钟一次，一天，最多不可能大于1w次
        List<AwemeDto> awemeDtos =
            awemeRepository.listForPage(dateBetween.getLeft(), queryBuilder(dateBetween, criteria), update_time, 10000,
                AwemeDto.class);

        AwemeDetailDto awemeDetailDto = new AwemeDetailDto();

        TreeMap<Date, Long> collect = Safes.of(awemeDtos)
            .parallelStream()
            .collect(Collectors.toMap(AwemeDto::getUpdate_time, AwemeDto::getDigg_count, (last, next) -> next,
                TreeMap::new));

        TreeMap<Date, Integer> collect1 = new TreeMap<>();
        for (AwemeDto awemeDto : awemeDtos) {
            Integer sales = 0;
            if (null != awemeDto.getExtra() && awemeDto.getExtra()
                .size() > 0) {
                if (null != awemeDto.getExtra()
                    .get(0)
                    .getSales()) {
                    // TODO: 2022/3/11 只支持展示一个
                    sales = awemeDto.getExtra()
                        .get(0)
                        .getSales();
                }
            }
            collect1.put(awemeDto.getUpdate_time(), sales);
        }

        awemeDetailDto.setXAxisData(collect.keySet());
        awemeDetailDto.setDigg_count(collect.values()
            .stream()
            .collect(Collectors.toList()));

        // TODO: 2022/3/10 时间怎么办，如果挂车的点赞怎么办，是不是没有的时候要补空
        awemeDetailDto.setKey1(collect1.keySet());
        awemeDetailDto.setSales(collect1.values()
            .stream()
            .collect(Collectors.toList()));

        return awemeDetailDto;
    }

    /**
     * 1、先根据用户 作品聚合，查看2小时内的数据
     * 2、再去拿这些数据去对比2小时外的数据，有没有增长
     *
     * @param criteria
     * @return
     */
    @SuppressWarnings("all")
    public void saveResults(AwemeLikeQueryCriteria criteria) {
        long start_1 = System.currentTimeMillis();

        Pair<String, String> dateBetween = switchDateBetween(criteria.getDateBetweenEnum());
        log.info("当前dateBetween === {},{}", dateBetween.getLeft(), dateBetween.getRight());

        long start = System.currentTimeMillis();

        /**
         * 这个n要业务处理一下
         * 1000个用户 同一批次 返回也就是 1000条记录
         * todo  每个用户的作品22 x 1000 = 22000条记录
         *
         * 但是22000跨索引就不行了，因为上边是倒叙
         */
        Integer length = 31680000;
        List<AwemeDto> awemeDtos = Lists.newArrayListWithExpectedSize(length + length);
        // TODO: 2022/3/9  每次查2遍，一次正序，一次倒叙就可以解决问题
        awemeDtos.addAll(
            awemeRepository.aggrList(dateBetween.getLeft(), queryBuilder(dateBetween, null), length, SortOrder.DESC));
        awemeDtos.addAll(
            awemeRepository.aggrList(dateBetween.getLeft(), queryBuilder(dateBetween, null), length, SortOrder.ASC));

        long end = System.currentTimeMillis();
        log.info("查询耗时：" + (end - start) + "ms", "查询数据长度：{}", awemeDtos.size());

        Map<Long, List<AwemeDto>> awemeMaps = Safes.of(awemeDtos)
            .parallelStream()
            .collect(Collectors.groupingBy(AwemeDto::getAweme_id));

        List<AwemeDto> list = Lists.newArrayListWithExpectedSize(awemeDtos.size());

        for (Map.Entry<Long, List<AwemeDto>> entry : awemeMaps.entrySet()) {

            List<AwemeDto> valueList = entry.getValue()
                .parallelStream()
                //                .filter(f -> f.getDigg_count() > 0)
                .collect(Collectors.toList());

            if (valueList.size() <= 1) {
                continue;
            }

            // 取最新的一条
            AwemeDto awemeDto = Safes.of(valueList)
                .parallelStream()
                .sorted(Comparator.comparing(AwemeDto::getUpdate_time)
                    .reversed())
                .findFirst()
                .orElse(null);

            Long max = valueList.parallelStream()
                .max(Comparator.comparing(AwemeDto::getDigg_count))
                .map(AwemeDto::getDigg_count)
                .orElse(0L);

            Long min = valueList.parallelStream()
                .min(Comparator.comparing(AwemeDto::getDigg_count))
                .map(AwemeDto::getDigg_count)
                .orElse(0L);

            long diff = max - min;

            try {
                // 按当前最新的商品列表来对比历史
                // TODO: 2022/3/5 最新的数据，没有挂车就不对比销量了
                if (null != awemeDto.getWith_goods() && awemeDto.getWith_goods()) {
                    if (Objects.nonNull(awemeDto.getExtra())) {

                        // 历史所有的
                        List<AwemeExtraDto> extraList = valueList.parallelStream()
                            .filter(f -> null != f.getWith_goods() && f.getWith_goods())
                            .filter(f -> null != f.getExtra())
                            .flatMap(map -> map.getExtra()
                                .stream())
                            .filter(f -> null != f.getSales())
                            .collect(Collectors.toList());

                        Map<String, List<AwemeExtraDto>> productGroup = Safes.of(extraList)
                            .parallelStream()
                            .collect(Collectors.groupingBy(AwemeExtraDto::getProduct_id));

                        List<SalesDiffDto> salesDiffDtoList = Lists.newArrayListWithExpectedSize(awemeDto.getExtra()
                            .size());

                        // 外层是当前的
                        for (AwemeExtraDto awemeExtraDto : awemeDto.getExtra()) {

                            SalesDiffDto salesDiffDto = new SalesDiffDto();
                            int sales_diff = 0;
                            // 内层是所有历史的
                            if (productGroup.containsKey(awemeExtraDto.getProduct_id())) {
                                List<AwemeExtraDto> awemeExtraDtos = productGroup.get(awemeExtraDto.getProduct_id());

                                // 最大1
                                Integer minSales = awemeExtraDtos.parallelStream()
                                    .min(Comparator.comparing(AwemeExtraDto::getSales))
                                    .map(AwemeExtraDto::getSales)
                                    .orElse(0);
                                // 当前 - 最小
                                if (null != awemeExtraDto.getSales()) {
                                    sales_diff = awemeExtraDto.getSales() - minSales;
                                }

                            }

                            salesDiffDto.setSales_diff(sales_diff);
                            salesDiffDto.setTitle(awemeExtraDto.getTitle());
                            salesDiffDto.setProduct_id(awemeExtraDto.getProduct_id());
                            salesDiffDtoList.add(salesDiffDto);
                        }
                        awemeDto.setSales_diff(salesDiffDtoList.size() > 0 ? salesDiffDtoList.get(0)
                            .getSales_diff() : 0);
                        awemeDto.setSalesDiff(salesDiffDtoList);
                    }
                }
            } catch (Exception e) {
                log.error("异常挂车商品：{}", JSON.toJSON(valueList), e);
            }

            awemeDto.setDiff(diff);
            list.add(awemeDto);
        }

        List<AwemeDto> collect = list.parallelStream()
            // 之前只判断点赞的diff是不对的，|| 判断销售的diff
            .filter(f -> f.getDiff() > 0 || (null != f.getSales_diff() && f.getSales_diff() > 0))
            .sorted(Comparator.comparing(AwemeDto::getDiff)
                .reversed())
            .collect(Collectors.toList());

        if (null != criteria.getWith_goods()) {
            if (criteria.getWith_goods()) {
                collect = collect.parallelStream()
                    .filter(f -> null != f.getWith_goods())
                    .filter(f -> f.getWith_goods())
                    .collect(Collectors.toList());
            } else {
                collect = collect.parallelStream()
                    .filter(f -> (null == f.getWith_goods()) || (null != f.getWith_goods() && !f.getWith_goods()))
                    .collect(Collectors.toList());
            }
        }

        long end_1 = System.currentTimeMillis();
        log.info("总耗时：" + (end_1 - start_1) + "ms");

        List<AwemeResultDto> collect1 = Safes.of(collect)
            .parallelStream()
            .map(map -> {
                AwemeResultDto awemeResultDto = new AwemeResultDto();
                BeanUtils.copyProperties(map, awemeResultDto);
                if (null == map.getWith_goods()) {
                    awemeResultDto.setWith_goods(false);
                }
                return awemeResultDto;
            })
            .collect(Collectors.toList());

        // 直接存储
        switch (criteria.getDateBetweenEnum()) {
            case TWO_HOUR:
                awemeResult2Repository.deleteIndex();
                awemeResult2Repository.insert(collect1,
                    "dm_aweme_result" + "_" + DateUtil.formatDate(DateUtil.getCurrentDate()) + "_2");
                return;
            case FOUR_HOUR:
                awemeResult4Repository.deleteIndex();
                awemeResult4Repository.insert(collect1,
                    "dm_aweme_result" + "_" + DateUtil.formatDate(DateUtil.getCurrentDate()) + "_4");
                return;
            case SIX_HOUR:
                awemeResult6Repository.deleteIndex();
                awemeResult6Repository.insert(collect1,
                    "dm_aweme_result" + "_" + DateUtil.formatDate(DateUtil.getCurrentDate()) + "_6");
                return;
            case TWELVE_HOUR:
                awemeResult12Repository.deleteIndex();
                awemeResult12Repository.insert(collect1,
                    "dm_aweme_result" + "_" + DateUtil.formatDate(DateUtil.getCurrentDate()) + "_12");
                return;
            case ONE_DAY:
                awemeResult24Repository.deleteIndex();
                awemeResult24Repository.insert(collect1,
                    "dm_aweme_result" + "_" + DateUtil.formatDate(DateUtil.getCurrentDate()) + "_24");
                return;
            default:
        }
    }

    public BoolQueryBuilder queryBuilder(Pair<String, String> date, AwemeLikeQueryCriteria criteria) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        boolQueryBuilder.filter(QueryBuilders.rangeQuery("update_time")
            .gte(date.getLeft())
            .lte(date.getRight()));

        if (null != criteria) {
            if (StringUtils.isNotEmpty(criteria.getStr_aweme_id())) {
                boolQueryBuilder.must(QueryBuilders.termQuery("aweme_id", Long.valueOf(criteria.getStr_aweme_id())));
            }
        }

        log.info(boolQueryBuilder);
        return boolQueryBuilder;
    }

    private List<Long> getAuthorUserId() {
        // 查一下白名单
        List<AwemeWhitelistDto> awemeWhitelistDtos =
            awemeWhitelistRepository.listForPage(null, null, 100000, AwemeWhitelistDto.class);
        List<Long> collect = Safes.of(awemeWhitelistDtos)
            .stream()
            .map(AwemeWhitelistDto::getUid)
            .collect(Collectors.toList());

        List<Long> collect1 = Lists.newArrayList();
        if (CollectionUtil.isNotEmpty(collect)) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            // 获取当前用户下绑定的所有用户
            boolQueryBuilder.must(QueryBuilders.termsQuery("user_id", collect));

            // TODO: 2022/3/13 最多1000个用户
            List<UserMonitorDto> userMonitorDtos =
                userMonitorRepository.listForPage(boolQueryBuilder, null, 1000, UserMonitorDto.class);

            collect1 = Safes.of(userMonitorDtos)
                .parallelStream()
                .map(UserMonitorDto::getUid)
                .collect(Collectors.toList());

            log.info("queryBuilder awemeWhitelist collect = {} ,collect1 = {} ", collect.size(), collect1.size());
        }
        return collect1;
    }

    public BoolQueryBuilder queryBuilder(AwemeLikeQueryCriteria criteria) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        Long currentUserId = SecurityUtils.getCurrentUserId();
        if (currentUserId != 1) {
            boolQueryBuilder.must(QueryBuilders.termsQuery("author_user_id", userMonitorService.getAuthorUserId()));
        } else {
            boolQueryBuilder.mustNot(QueryBuilders.termsQuery("author_user_id", getAuthorUserId()));
        }

        if (StringUtils.isNoneBlank(criteria.getNickname())) {
            boolQueryBuilder.must(QueryBuilders.termQuery("nickname", criteria.getNickname()));
        }

        if (StringUtils.isNoneBlank(criteria.getDesc())) {
            boolQueryBuilder.must(QueryBuilders.wildcardQuery("desc", "*" + criteria.getDesc() + "*"));
        }

        if (Objects.nonNull(criteria.getWith_goods())) {
            boolQueryBuilder.must(QueryBuilders.termQuery("with_goods", criteria.getWith_goods()));
        }

        if (Objects.nonNull(criteria.getUnique_id())) {
            boolQueryBuilder.must(QueryBuilders.termQuery("unique_id", criteria.getUnique_id()));
        }
        if (null != criteria.getCreate_time() && criteria.getCreate_time().length > 0) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("create_time")
                .gte(criteria.getCreate_time()[0])
                .lte(criteria.getCreate_time()[1]));
        }

        log.info(boolQueryBuilder);
        return boolQueryBuilder;
    }

    /**
     * 列表用
     */
    private Pair<String, String> switchDateBetween(DateBetweenEnum dateBetweenEnum) {
        switch (dateBetweenEnum) {
            case TWO_HOUR:
                return Pair.of(DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDateTime(), -2),
                    DateUtil.FORMAT_DATE_TIME),
                    DateUtil.formatDate(DateUtil.getCurrentDateTime(), DateUtil.FORMAT_DATE_TIME));
            case FOUR_HOUR:
                return Pair.of(DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDateTime(), -4),
                    DateUtil.FORMAT_DATE_TIME),
                    DateUtil.formatDate(DateUtil.getCurrentDateTime(), DateUtil.FORMAT_DATE_TIME));
            case SIX_HOUR:
                return Pair.of(DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDateTime(), -6),
                    DateUtil.FORMAT_DATE_TIME),
                    DateUtil.formatDate(DateUtil.getCurrentDateTime(), DateUtil.FORMAT_DATE_TIME));
            case TWELVE_HOUR:
                return Pair.of(DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDateTime(), -12),
                    DateUtil.FORMAT_DATE_TIME),
                    DateUtil.formatDate(DateUtil.getCurrentDateTime(), DateUtil.FORMAT_DATE_TIME));
            case ONE_DAY:
                return Pair.of(DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDateTime(), -24),
                    DateUtil.FORMAT_DATE_TIME),
                    DateUtil.formatDate(DateUtil.getCurrentDateTime(), DateUtil.FORMAT_DATE_TIME));
            case THREE_DAY:
                return Pair.of(
                    DateUtil.formatDate(DateUtil.addDays(DateUtil.getCurrentDateTime(), -2), DateUtil.FORMAT_DATE_TIME),
                    DateUtil.formatDate(DateUtil.getCurrentDateTime(), DateUtil.FORMAT_DATE_TIME));
            default:
                return Pair.of(DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDateTime(), -2),
                    DateUtil.FORMAT_DATE_TIME),
                    DateUtil.formatDate(DateUtil.getCurrentDateTime(), DateUtil.FORMAT_DATE_TIME));
        }
    }

    /**
     * 详情用
     */
    private Pair<String, String> switchDateBetweenDetail(DateBetweenEnum dateBetweenEnum) {
        switch (dateBetweenEnum) {
            case TWO_HOUR:
                return Pair.of(
                    DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDate(), -2), DateUtil.DATE_FORMAT),
                    DateUtil.formatDate(DateUtil.getCurrentDate(), DateUtil.DATE_FORMAT));
            case FOUR_HOUR:
                return Pair.of(
                    DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDate(), -4), DateUtil.DATE_FORMAT),
                    DateUtil.formatDate(DateUtil.getCurrentDate(), DateUtil.DATE_FORMAT));
            case SIX_HOUR:
                return Pair.of(
                    DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDate(), -6), DateUtil.DATE_FORMAT),
                    DateUtil.formatDate(DateUtil.getCurrentDate(), DateUtil.DATE_FORMAT));
            case TWELVE_HOUR:
                return Pair.of(
                    DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDate(), -12), DateUtil.DATE_FORMAT),
                    DateUtil.formatDate(DateUtil.getCurrentDate(), DateUtil.DATE_FORMAT));
            case ONE_DAY:
                return Pair.of(
                    DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDate(), -24), DateUtil.DATE_FORMAT),
                    DateUtil.formatDate(DateUtil.getCurrentDate(), DateUtil.DATE_FORMAT));
            case THREE_DAY:
                return Pair.of(
                    DateUtil.formatDate(DateUtil.addDays(DateUtil.getCurrentDate(), -2), DateUtil.DATE_FORMAT),
                    DateUtil.formatDate(DateUtil.getCurrentDate(), DateUtil.DATE_FORMAT));
            default:
                return Pair.of(
                    DateUtil.formatDate(DateUtil.addHours(DateUtil.getCurrentDate(), -2), DateUtil.DATE_FORMAT),
                    DateUtil.formatDate(DateUtil.getCurrentDate(), DateUtil.DATE_FORMAT));
        }
    }

}
