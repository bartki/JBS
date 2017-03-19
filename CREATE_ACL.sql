
DECLARE
    acl_name          VARCHAR2 (30) := 'utl_tcp.xml';
    ftp_server_ip     VARCHAR2 (20) := '193.202.117.201';
    ftp_server_name   VARCHAR2 (20) := 'INFINITE_FTP';
    username          VARCHAR2 (30) := 'TETA_ADMIN';
BEGIN
    BEGIN
        DBMS_NETWORK_ACL_ADMIN.drop_acl (acl => acl_name);
    EXCEPTION
        WHEN OTHERS
        THEN
            NULL;
    END;

    DBMS_NETWORK_ACL_ADMIN.create_acl (acl           => acl_name,
                                       description   => 'FTP INFINITE Access',
                                       principal     => username,
                                       is_grant      => TRUE,
                                       privilege     => 'connect',
                                       start_date    => NULL,
                                       end_date      => NULL);

    COMMIT;



    DBMS_NETWORK_ACL_ADMIN.add_privilege (acl          => acl_name,
                                          principal    => username,
                                          is_grant     => TRUE,
                                          privilege    => 'connect',
                                          start_date   => NULL,
                                          end_date     => NULL);
    COMMIT;

    DBMS_NETWORK_ACL_ADMIN.assign_acl (acl          => acl_name,
                                       HOST         => ftp_server_name,
                                       lower_port   => NULL,
                                       upper_port   => NULL);
    COMMIT;

    DBMS_NETWORK_ACL_ADMIN.assign_acl (acl          => acl_name,
                                       HOST         => ftp_server_ip,
                                       lower_port   => NULL,
                                       upper_port   => NULL);
    COMMIT;
END;
/
