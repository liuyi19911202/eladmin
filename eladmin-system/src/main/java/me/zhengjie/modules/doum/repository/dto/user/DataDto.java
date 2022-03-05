package me.zhengjie.modules.doum.repository.dto.user;

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
public class DataDto {
    private Long uid;
    private String nickname;
    private String unique_id;
    private Long aweme_count;
    private Long follower_count;
    private AvatarMediumDto avatar_medium;
}
