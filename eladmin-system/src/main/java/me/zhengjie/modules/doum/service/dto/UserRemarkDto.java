package me.zhengjie.modules.doum.service.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;

/**
 * @author liuyi
 * @date 2022/2/22
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Document(indexName = "dm_user_remark", type = "_doc")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserRemarkDto {
    /**
     * 当前登录用户
     */
    private Long user_id;
    /**
     * uid
     */
    private Long uid;

    private String remark;
}
