package me.zhengjie.modules.doum.service;

import me.zhengjie.modules.doum.service.dto.AwemeQueryCriteria;
import org.springframework.data.domain.Pageable;

public interface AwemeService {

    Object list(AwemeQueryCriteria criteria, Pageable pageable);

    Object newList(AwemeQueryCriteria criteria, Pageable pageable);
}
