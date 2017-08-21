package com.huawei.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Created by shen on 8/21/17.
 */

// Just a fake LoginUtil. Please use proper one to replace it.
public class LoginUtil {
    private static final Logger LOG = Logger.getLogger(LoginUtil.class);
    public synchronized static void login(String userPrincipal, String userKeytabPath, String krb5ConfPath, Configuration conf)
        throws Exception
    {
        LOG.info("Fake LoginUtil is triggered");
    }
}
