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
public class UserInfoDto {
    private DataDto data;
    private Integer code;
    private String message;
}
