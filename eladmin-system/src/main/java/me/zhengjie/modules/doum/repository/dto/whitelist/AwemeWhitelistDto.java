package me.zhengjie.modules.doum.repository.dto.whitelist;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;

/**
 * @author liuyi
 * @date 2022/4/20
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Document(indexName = "dm_aweme_whitelist", type = "_doc")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AwemeWhitelistDto {
    private Long uid;
}
