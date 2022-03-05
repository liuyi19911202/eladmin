package me.zhengjie.modules.doum.repository.dto.aweme;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author liuyi
 * @date 2022/3/1
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AwemeListObjectDto {
    private Long author_user_id;
    private AuthorDto author;
    private String desc;
    private StatisticsDto statistics;
    private StatusDto status;
    private AnchorInfoDto anchor_info;
    private Long aweme_id;
    private String share_url;
    private int create_time;
}
