--SELECT host, lower_port, upper_port, acl
--  FROM dba_network_acls

DECLARE
    acl_name             VARCHAR2(30) := 'utl_tcp.xml';
    ftp_server_ip        VARCHAR2(20) := '85.17.73.180';
    ftp_server_name      VARCHAR2(20) := 'jurag.cba.pl';
    username             VARCHAR2(30) := 'PM_DEVELOP_ADM';
BEGIN
    dbms_network_acl_admin.create_acl (
        acl         => acl_name,
        description => 'FTP INFINITE Access',
        principal   => username,
        is_grant    => TRUE,
        privilege   => 'connect',
        start_date  => null,
        end_date    => null);
    commit;

    dbms_network_acl_admin.add_privilege (
        acl        => acl_name,
        principal  => username,
        is_grant   => TRUE,
        privilege  => 'connect',
        start_date => null,
        end_date   => null);
    commit;

    dbms_network_acl_admin.assign_acl (
        acl        => acl_name,
        host       => ftp_server_name,
        lower_port => NULL,
        upper_port => NULL);
    commit;
    
    dbms_network_acl_admin.assign_acl (
        acl        => acl_name,
        host       => ftp_server_ip,
        lower_port => NULL,
        upper_port => NULL);
    commit;
END;
