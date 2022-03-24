package me.zhengjie.modules.doum.service.impl;

import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.cdos.api.bean.PageInfo;
import com.cdos.api.bean.cdosapi.CdosApiPageResponse;
import com.cdos.utils.DateUtil;
import com.cdos.utils.Safes;
import com.cdos.utils.json.JacksonProvider;
import com.cdos.web.htppclient.CdosHttpRestTemplate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import me.zhengjie.modules.doum.repository.UserMonitorRepository;
import me.zhengjie.modules.doum.repository.UserRemarkRepository;
import me.zhengjie.modules.doum.repository.dto.user.DataDto;
import me.zhengjie.modules.doum.repository.dto.user.UserInfoDto;
import me.zhengjie.modules.doum.service.UserMonitorService;
import me.zhengjie.modules.doum.service.dto.UserMonitorDto;
import me.zhengjie.modules.doum.service.dto.UserMonitorQueryCriteria;
import me.zhengjie.modules.doum.service.dto.UserRemarkDto;
import me.zhengjie.utils.PageUtil;
import me.zhengjie.utils.SecurityUtils;
import me.zhengjie.utils.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
@CacheConfig(cacheNames = "usermonitor")
public class UserMonitorServiceImpl implements UserMonitorService {
    @Autowired
    UserMonitorRepository userMonitorRepository;
    @Autowired
    CdosHttpRestTemplate userHttpRestTemplate;
    @Autowired
    UserRemarkRepository userRemarkRepository;

    @Override
    public Object list(UserMonitorQueryCriteria criteria, Pageable pageable) {
        FieldSortBuilder sortBuilder = new FieldSortBuilder("create_time").order(SortOrder.DESC);

        CdosApiPageResponse<UserMonitorDto> awemeDtoCdosApiPageResponse =
            userMonitorRepository.listForPage(new PageInfo(pageable.getPageNumber() + 1, pageable.getPageSize()),
                queryBuilder(criteria), sortBuilder, UserMonitorDto.class);

        Long currentUserId = SecurityUtils.getCurrentUserId();
        List<UserMonitorDto> collect = Safes.of(awemeDtoCdosApiPageResponse.getResult())
            .parallelStream()
            .map(map -> {
                UserRemarkDto remarkByUid = getRemarkByUid(map.getUid(), currentUserId);
                if (null != remarkByUid) {
                    map.setRemark(remarkByUid.getRemark());
                }
                map.setId(map.getUid());
                return map;
            })
            .collect(Collectors.toList());

        return PageUtil.toPage(collect, awemeDtoCdosApiPageResponse.getPage()
            .getTotalCount());
    }

    @Override
    public Object add(UserMonitorDto dto) {
        userMonitorRepository.insert(dto);
        return Boolean.TRUE;
    }

    @Override
    @SneakyThrows
    public Object getUser1(UserMonitorQueryCriteria criteria) {
        String[] split = criteria.getHome_url()
            .split("\n");

        Long currentUserId = SecurityUtils.getCurrentUserId();
        for (String i : split) {

            String home_url = i.substring(i.indexOf("https"));
            String s = HttpUtil.get(home_url);
            int start = s.indexOf("user");
            int end = s.indexOf("?");
            String substring = s.substring(start, end);
            // 用户的sec_user_id信息
            String sec_user_id = substring.substring(5);

            Map<String, String> params = Maps.newHashMapWithExpectedSize(3);
            params.put("sec_uid", sec_user_id);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
            headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON_UTF8));
            headers.set("token", "test");
            String jsonParam = JacksonProvider.getObjectMapper()
                .writeValueAsString(Objects.nonNull(params) ? params : "");

            HttpEntity<String> requestEntity = new HttpEntity<>(jsonParam, headers);

            String userStr = userHttpRestTemplate.getDelegate()
                .postForObject("http://127.0.0.1:9822/api/aweme/userInfo", requestEntity, String.class);

            // 格式化
            UserInfoDto userInfoDto = JSON.parseObject(userStr, UserInfoDto.class);
            if (Objects.nonNull(userInfoDto)) {
                DataDto data = userInfoDto.getData();

                if (Objects.isNull(data.getUid())) {
                    log.info("当前sec_user_id = {} , data uid is null ", sec_user_id, JSON.toJSONString(data));
                }

                // 先查询一边有没有uid
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                boolQueryBuilder.must(QueryBuilders.termQuery("uid", data.getUid()));
                List<UserMonitorDto> userMonitorDtos =
                    userMonitorRepository.listForPage(boolQueryBuilder, null, 1, UserMonitorDto.class);
                if (null != userMonitorDtos && userMonitorDtos.size() >= 1) {
                    // 说明已有此用户，更新即可
                    Set<Long> user_id = userMonitorDtos.get(0)
                        .getUser_id();
                    if (null != user_id) {
                        user_id.add(currentUserId);
                        userMonitorDtos.get(0)
                            .setUser_id(user_id);
                        userMonitorRepository.insert(userMonitorDtos.get(0));
                    } else {
                        HashSet<Long> longs = Sets.newHashSet(currentUserId);
                        userMonitorDtos.get(0)
                            .setUser_id(longs);
                        userMonitorRepository.insert(userMonitorDtos.get(0));
                    }

                } else {
                    userMonitorRepository.insert(UserMonitorDto.builder()
                        // TODO: 2022/3/1 当前登录用户
                        .user_id(Sets.newHashSet(currentUserId))
                        .home_url(home_url)
                        .sec_user_id(sec_user_id)
                        .nickname(data.getNickname())
                        .uid(data.getUid())
                        .unique_id(data.getUnique_id())
                        .aweme_count(data.getAweme_count())
                        .follower_count(data.getFollower_count())
                        .avatar_medium(data.getAvatar_medium()
                            .getUrl_list())
                        .create_time(DateUtil.getCurrentDateTime())
                        .build());
                }
            }
        }
        return null;
    }

    @Override
    public void updateRemark(UserMonitorQueryCriteria criteria) {
        Long currentUserId = SecurityUtils.getCurrentUserId();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("user_id", currentUserId));
        boolQueryBuilder.must(QueryBuilders.termQuery("uid", criteria.getUid()));

        List<UserRemarkDto> uid = userRemarkRepository.listForPage(boolQueryBuilder, null, 1, UserRemarkDto.class);
        if (null != uid && uid.size() > 0) {
            log.info("已有备注：{},执行删除", criteria.getUid());
            userRemarkRepository.delete(boolQueryBuilder);
        }
        userRemarkRepository.insert(UserRemarkDto.builder()
            .uid(criteria.getUid())
            .user_id(currentUserId)
            .remark(criteria.getRemark())
            .build());
    }

    @Override
    public Object getRemark(UserMonitorQueryCriteria criteria) {
        UserRemarkDto remarkByUid = getRemarkByUid(criteria.getUid(), SecurityUtils.getCurrentUserId());
        if (null != remarkByUid) {
            return remarkByUid;
        }

        return UserRemarkDto.builder()
            .uid(criteria.getUid())
            .build();
    }

    public UserRemarkDto getRemarkByUid(Long uid, Long userId) {
        if (uid == null || userId == null) {
            log.info("getRemarkByUid is null {},{}", uid, userId);
            return null;
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        boolQueryBuilder.must(QueryBuilders.termQuery("user_id", userId));
        boolQueryBuilder.must(QueryBuilders.termQuery("uid", uid));

        List<UserRemarkDto> userRemarkDtos =
            userRemarkRepository.listForPage(boolQueryBuilder, null, 1, UserRemarkDto.class);
        if (null != uid && userRemarkDtos.size() > 0) {
            return userRemarkDtos.get(0);
        }
        return null;
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
    public Object delete(Set<Long> ids) {
        Long currentUserId = SecurityUtils.getCurrentUserId();
        for (Long id : ids) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.must(QueryBuilders.termQuery("uid", id));
            boolQueryBuilder.must(QueryBuilders.termsQuery("user_id", Sets.newHashSet(currentUserId)));
            List<UserMonitorDto> userMonitorDtos =
                userMonitorRepository.listForPage(boolQueryBuilder, null, 1, UserMonitorDto.class);
            if (null != userMonitorDtos && userMonitorDtos.size() > 0) {

                UserMonitorDto userMonitorDto = userMonitorDtos.get(0);
                Set<Long> user_id = userMonitorDto.getUser_id();
                // 删除
                log.info("当前user_id={},登陆用户={}", user_id, currentUserId);
                boolean remove = user_id.remove(currentUserId);
                if (remove) {
                    if (user_id.size() == 0) {
                        // 只有一个的时候，根据uid删除掉
                        userMonitorRepository.delete(QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery("uid", id)));

                        log.info("当前user_id == 0，删除用户:{}", id);
                    } else {
                        // 更新user_id
                        userMonitorDto.setUser_id(user_id);
                        userMonitorRepository.insert(userMonitorDto);
                        log.info("更新user_id，更新之后:{}", user_id);
                    }
                } else {
                    log.error("user_id ：{} , 当前登陆用户id：{} ，uid：{} --- 未查询到", currentUserId, id);
                    // 可能是因为admin，判断一下
                }
            }
        }
        return Boolean.TRUE;
    }

    public BoolQueryBuilder queryBuilder(UserMonitorQueryCriteria criteria) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        Long currentUserId = SecurityUtils.getCurrentUserId();
        if (currentUserId != 1) {
            boolQueryBuilder.must(QueryBuilders.termsQuery("user_id", Sets.newHashSet(currentUserId)));
        }

        if (StringUtils.isNoneBlank(criteria.getNickname())) {
            boolQueryBuilder.must(QueryBuilders.termQuery("nickname", criteria.getNickname()));
        }

        if (null != criteria.getUid()) {
            boolQueryBuilder.must(QueryBuilders.termQuery("uid", criteria.getUid()));
        }

        log.info(boolQueryBuilder);
        return boolQueryBuilder;
    }

    @Override
    public List<Long> getAuthorUserId() {
        // 获取当前用户下绑定的所有用户
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        boolQueryBuilder.must(QueryBuilders.termsQuery("user_id", Sets.newHashSet(SecurityUtils.getCurrentUserId())));

        // TODO: 2022/3/13 最多1000个用户
        List<UserMonitorDto> userMonitorDtos =
            userMonitorRepository.listForPage(boolQueryBuilder, null, 1000, UserMonitorDto.class);

        List<Long> collect = Safes.of(userMonitorDtos)
            .parallelStream()
            .map(UserMonitorDto::getUid)
            .collect(Collectors.toList());
        return collect;
    }
}
