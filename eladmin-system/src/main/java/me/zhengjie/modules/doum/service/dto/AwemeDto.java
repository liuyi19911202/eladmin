package me.zhengjie.modules.doum.service.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.Date;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Document(indexName = "dm_aweme", type = "_doc")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AwemeDto {
    private String desc;
    private String digg_count;
    private String collect_count;
    private String comment_count;
    private String share_count;
    private String share_url;
    @JsonFormat(locale = "zh", timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    private Date create_time;
    @JsonFormat(locale = "zh", timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    private Date update_time;
    private String author_user_id;
    private String nickname;
}
