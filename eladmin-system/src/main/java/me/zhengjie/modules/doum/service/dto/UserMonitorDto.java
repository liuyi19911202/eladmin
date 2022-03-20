package me.zhengjie.modules.doum.service.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Document(indexName = "dm_user_monitor", type = "_doc")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserMonitorDto {
    /**
     * 当前登录用户
     */
    private Set<Long> user_id;
    /**
     * 主页链接
     * <p>
     * 长按复制此条消息，打开抖音搜索，查看TA的更多作品。 https://v.douyin.com/LER6mF5/
     */
    private String home_url;
    /**
     * 用户的唯一信息
     */
    private String sec_user_id;
    /**
     * 账号名称
     */
    private String nickname;
    /**
     * uid
     */
    @Id
    private Long uid;
    /**
     * 抖音号
     */
    private String unique_id;
    /**
     * 作品数
     */
    private Long aweme_count;
    /**
     * 粉丝数
     */
    private Long follower_count;
    /**
     * 头像
     */
    private String[] avatar_medium;
    @JsonFormat(locale = "zh", timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    private Date create_time;

    private String remark;

    @Transient
    private Long id;
}
