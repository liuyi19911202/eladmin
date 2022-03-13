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

/**
 * @author liuyi
 * @date 2022/2/22
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Document(indexName = "dm_aweme_result_*", type = "_doc")
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AwemeResultDto {
    @Id
    private Long aweme_id;
    private String desc;
    /**
     * 点赞
     */
    private Long digg_count;
    private Long collect_count;
    private Long comment_count;
    private Long share_count;
    private String share_url;
    /**
     * 抖音号
     */
    private String unique_id;
    /**
     * 视频是否删除
     */
    private Boolean is_delete;
    @JsonFormat(locale = "zh", timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    private Date create_time;
    @JsonFormat(locale = "zh", timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    private Date update_time;
    private Long author_user_id;
    private String nickname;
    private Long user_id;

    // ----------------商品信息------------

    @Builder.Default

    private Boolean with_goods = false;
    private String[] product_url_list;
    private List<AwemeExtraDto> extra;

    // ----------------商品信息------------

    /**
     * 冗余前端返回，点赞差
     */
    private long diff;
    /**
     * 冗余一下 多个商品
     */
    private List<SalesDiffDto> salesDiff;
    /**
     * 销量增长
     */
    private Integer sales_diff;
    @Transient
    private List<AwemeResultDto> detailfList;

    /**
     * 冗余一个，浏览器丢了3位
     */
    @Transient
    private String str_aweme_id;
}
