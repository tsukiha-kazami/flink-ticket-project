package com.kkb.b2c.gciantispider.dao;

import com.kkb.b2c.gciantispider.model.Account;

import java.util.List;

public interface IAccountDao extends IBaseDao<Account> {
    
    public List<Account> roleUser(String[] ids, int pageNo, int pageSize);

    public Long roleUserCount(String[] ids);
}