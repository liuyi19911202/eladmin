package me.zhengjie.modules.doum.service;

import me.zhengjie.modules.doum.service.dto.UserMonitorDto;
import me.zhengjie.modules.doum.service.dto.UserMonitorQueryCriteria;
import org.springframework.data.domain.Pageable;

import java.util.Set;

public interface UserMonitorService {

    Object list(UserMonitorQueryCriteria criteria, Pageable pageable);

    Object add(UserMonitorDto dto);

    Object delete(Set<String> ids);

    Object getUser(UserMonitorQueryCriteria criteria);
}
