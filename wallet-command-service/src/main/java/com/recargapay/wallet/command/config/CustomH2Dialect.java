package com.recargapay.wallet.command.config;

import org.hibernate.dialect.H2Dialect;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;

public class CustomH2Dialect extends H2Dialect {

    public CustomH2Dialect() {
        System.out.println("CustomH2Dialect initialized");
    }

    @Override
    public String appendLockHint(LockOptions lockOptions, String sql) {
        if (lockOptions.getLockMode() == LockMode.PESSIMISTIC_WRITE || lockOptions.getLockMode() == LockMode.PESSIMISTIC_READ) {
            return sql + " for update";
        }
        return sql;
    }
}