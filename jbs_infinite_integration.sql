CREATE TABLE jg_sql_repository
(
    id              NUMBER (10, 0),
    object_type     VARCHAR2 (30) NOT NULL,
    sql_query       CLOB,
    xslt            CLOB,
    file_location   VARCHAR2 (250),
    up_to_date      VARCHAR2 (1 BYTE) DEFAULT 'T' NOT NULL,
    direction       VARCHAR2 (3)
)
/

ALTER TABLE jg_sql_repository
    ADD CONSTRAINT jg_sqre_pk PRIMARY KEY (id)
/

ALTER TABLE jg_sql_repository
    ADD CONSTRAINT jg_sqre_object_type UNIQUE (object_type)
/

CREATE SEQUENCE jg_sqre_seq MINVALUE 1
                            MAXVALUE 9999999999999999999999999999
                            START WITH 1
                            INCREMENT BY 1
                            CACHE 20
/

CREATE TABLE jg_observed_operations
(
    id               NUMBER (10, 0),
    object_type      VARCHAR2 (30),
    object_id        NUMBER (10, 0),
    operation_type   VARCHAR2 (6),
    batch_guid       VARCHAR2 (40)
)
/

ALTER TABLE jg_observed_operations
    ADD CONSTRAINT jg_obop_pk PRIMARY KEY (id)
/

ALTER TABLE jg_observed_operations
    ADD CONSTRAINT jg_obop_operation_type CHECK
            (operation_type IN ('INSERT',
                                'UPDATE',
                                'DELETE',
                                'SKIPPED',
                                'NO_DATA'))
/

ALTER TABLE jg_observed_operations
    ADD CONSTRAINT jg_obop_object_id_type UNIQUE (object_type, object_id)
/

CREATE SEQUENCE jg_obop_seq MINVALUE 1
                            MAXVALUE 9999999999999999999999999999
                            START WITH 1
                            INCREMENT BY 1
                            CACHE 20
/

CREATE TABLE jg_output_log
(
    id            NUMBER (10, 0),
    guid          VARCHAR2 (32) DEFAULT SYS_GUID (),
    log_date      DATE DEFAULT SYSDATE,
    object_type   VARCHAR2 (30) NOT NULL,
    status        VARCHAR2 (9) DEFAULT 'READY',
    xml           CLOB,
    error         CLOB,
    file_name     VARCHAR2 (100)
)
/

ALTER TABLE jg_output_log
    ADD CONSTRAINT jg_oulo_pk PRIMARY KEY (id)
/

ALTER TABLE jg_output_log
    ADD CONSTRAINT jg_oulo_status CHECK
            (status IN ('READY',
                        'PROCESSED',
                        'ERROR',
                        'SKIPPED',
                        'NO_DATA'))
/

CREATE SEQUENCE jg_oulo_seq MINVALUE 1
                            MAXVALUE 9999999999999999999999999999
                            START WITH 1
                            INCREMENT BY 1
                            CACHE 20
/

BEGIN
    api_pa_obie.register_table (p_object_name      => 'JG_SQL_REPOSITORY',
                                p_subsystem_code   => 'PA',
                                p_alias            => 'SQRE');
    api_pa_obie.register_table (p_object_name      => 'JG_OBSERVED_OPERATIONS',
                                p_subsystem_code   => 'PA',
                                p_alias            => 'JOBOP');
    api_pa_obie.register_table (p_object_name      => 'JG_OUTPUT_LOG',
                                p_subsystem_code   => 'PA',
                                p_alias            => 'OULO');
    api_pa_obie.register_sequence (p_object_name      => 'JG_OBOP_SEQ',
                                   p_subsystem_code   => 'PA');
    api_pa_obie.register_sequence (p_object_name      => 'JG_OULO_SEQ',
                                   p_subsystem_code   => 'PA');
    api_pa_obie.register_sequence (p_object_name      => 'JG_SQRE_SEQ',
                                   p_subsystem_code   => 'PA');
    api_pa_obie.register_package (p_object_name      => 'JG_FTP',
                                  p_subsystem_code   => 'PA');
    api_pa_obie.register_package (p_object_name      => 'JG_FTP_CONFIGURATION',
                                  p_subsystem_code   => 'PA');
    api_pa_obie.register_package (p_object_name      => 'JG_INPUT_SYNC',
                                  p_subsystem_code   => 'PA');
    api_pa_obie.register_package (p_object_name      => 'JG_OUTPUT_SYNC',
                                  p_subsystem_code   => 'PA');
    api_pa_obie.register_package (p_object_name      => 'JG_OBOP_DEF',
                                  p_subsystem_code   => 'PA');
END;
/

ALTER TABLE jg_observed_operations
    ADD attachment VARCHAR2 (1) DEFAULT 'N' NOT NULL
/

ALTER TABLE jg_observed_operations
    ADD CONSTRAINT jg_obop_attachment_ck CHECK (attachment IN ('N', 'T'))
/

-----------------------

CREATE TABLE jg_input_log
(
    id               NUMBER (10, 0),
    log_date         DATE DEFAULT SYSDATE,
    file_name        VARCHAR2 (100),
    object_type      VARCHAR2 (30),
    xml              CLOB,
    on_time          VARCHAR2 (1),
    status           VARCHAR2 (9) DEFAULT 'READY',
    processed_date   DATE,
    error            CLOB,
    object_id        NUMBER (10, 0),
    xml_response     CLOB
)
/

ALTER TABLE jg_input_log
    ADD CONSTRAINT jg_inlo_pk PRIMARY KEY (id)
/

ALTER TABLE jg_input_log
    ADD CONSTRAINT jg_inlo_status_ck CHECK
            (status IN ('READY', 'PROCESSED', 'ERROR'))
/

ALTER TABLE jg_input_log
    ADD CONSTRAINT jg_inlo_on_time_ck CHECK (on_time IN ('N', 'T'))
/

CREATE SEQUENCE jg_inlo_seq MINVALUE 1
                            MAXVALUE 9999999999999999999999999999
                            START WITH 1
                            INCREMENT BY 1
                            CACHE 20
/

BEGIN
    api_pa_obie.register_table (p_object_name      => 'JG_INPUT_LOG',
                                p_subsystem_code   => 'PA',
                                p_alias            => 'INLO');
    api_pa_obie.register_sequence (p_object_name      => 'JG_INLO_SEQ',
                                   p_subsystem_code   => 'PA');
END;
/

-----------------------

CREATE OR REPLACE PACKAGE jg_ftp
AS
    ------------------------------------------------------------------------------------------------------------------------
    TYPE t_string_table IS TABLE OF VARCHAR2 (32767);

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION login (p_host      IN VARCHAR2,
                    p_port      IN VARCHAR2,
                    p_user      IN VARCHAR2,
                    p_pass      IN VARCHAR2,
                    p_timeout   IN NUMBER := NULL)
        RETURN UTL_TCP.connection;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_passive (p_conn IN OUT NOCOPY UTL_TCP.connection)
        RETURN UTL_TCP.connection;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE LOGOUT (p_conn    IN OUT NOCOPY UTL_TCP.connection,
                      p_reply   IN            BOOLEAN := TRUE);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_command (p_conn      IN OUT NOCOPY UTL_TCP.connection,
                            p_command   IN            VARCHAR2,
                            p_reply     IN            BOOLEAN := TRUE);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get_reply (p_conn IN OUT NOCOPY UTL_TCP.connection);

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_local_ascii_data (p_dir IN VARCHAR2, p_file IN VARCHAR2)
        RETURN CLOB;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_local_binary_data (p_dir IN VARCHAR2, p_file IN VARCHAR2)
        RETURN BLOB;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_remote_ascii_data (
        p_conn   IN OUT NOCOPY UTL_TCP.connection,
        p_file   IN            VARCHAR2)
        RETURN CLOB;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_remote_binary_data (
        p_conn   IN OUT NOCOPY UTL_TCP.connection,
        p_file   IN            VARCHAR2)
        RETURN BLOB;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put_local_ascii_data (p_data   IN CLOB,
                                    p_dir    IN VARCHAR2,
                                    p_file   IN VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put_local_binary_data (p_data   IN BLOB,
                                     p_dir    IN VARCHAR2,
                                     p_file   IN VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put_remote_ascii_data (
        p_conn   IN OUT NOCOPY UTL_TCP.connection,
        p_file   IN            VARCHAR2,
        p_data   IN            CLOB);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put_remote_binary_data (
        p_conn   IN OUT NOCOPY UTL_TCP.connection,
        p_file   IN            VARCHAR2,
        p_data   IN            BLOB);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get (p_conn        IN OUT NOCOPY UTL_TCP.connection,
                   p_from_file   IN            VARCHAR2,
                   p_to_dir      IN            VARCHAR2,
                   p_to_file     IN            VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put (p_conn        IN OUT NOCOPY UTL_TCP.connection,
                   p_from_dir    IN            VARCHAR2,
                   p_from_file   IN            VARCHAR2,
                   p_to_file     IN            VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get_direct (p_conn        IN OUT NOCOPY UTL_TCP.connection,
                          p_from_file   IN            VARCHAR2,
                          p_to_dir      IN            VARCHAR2,
                          p_to_file     IN            VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put_direct (p_conn        IN OUT NOCOPY UTL_TCP.connection,
                          p_from_dir    IN            VARCHAR2,
                          p_from_file   IN            VARCHAR2,
                          p_to_file     IN            VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE HELP (p_conn IN OUT NOCOPY UTL_TCP.connection);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE ASCII (p_conn IN OUT NOCOPY UTL_TCP.connection);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE binary (p_conn IN OUT NOCOPY UTL_TCP.connection);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE list (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                    p_dir    IN            VARCHAR2,
                    p_list      OUT        t_string_table);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE nlst (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                    p_dir    IN            VARCHAR2,
                    p_list      OUT        t_string_table);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE rename (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                      p_from   IN            VARCHAR2,
                      p_to     IN            VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE delete (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                      p_file   IN            VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE mkdir (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                     p_dir    IN            VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE rmdir (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                     p_dir    IN            VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE convert_crlf (p_status IN BOOLEAN);
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE PACKAGE BODY jg_ftp
AS
    ------------------------------------------------------------------------------------------------------------------------
    g_reply          t_string_table := t_string_table ();
    g_binary         BOOLEAN := TRUE;
    g_debug          BOOLEAN := TRUE;
    g_convert_crlf   BOOLEAN := TRUE;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE debug (p_text IN VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION login (p_host      IN VARCHAR2,
                    p_port      IN VARCHAR2,
                    p_user      IN VARCHAR2,
                    p_pass      IN VARCHAR2,
                    p_timeout   IN NUMBER := NULL)
        RETURN UTL_TCP.connection
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn   UTL_TCP.connection;
    BEGIN
        g_reply.delete;
        l_conn :=
            UTL_TCP.open_connection (p_host, p_port, tx_timeout => p_timeout);
        get_reply (l_conn);
        send_command (l_conn, 'USER ' || p_user);
        send_command (l_conn, 'PASS ' || p_pass);
        RETURN l_conn;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_passive (p_conn IN OUT NOCOPY UTL_TCP.connection)
        RETURN UTL_TCP.connection
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn    UTL_TCP.connection;
        l_reply   VARCHAR2 (32767);
        --l_host    VARCHAR(100);
        l_port1   NUMBER (10);
        l_port2   NUMBER (10);
    BEGIN
        send_command (p_conn, 'PASV');
        l_reply := g_reply (g_reply.LAST);
        l_reply :=
            REPLACE (
                SUBSTR (l_reply,
                        INSTR (l_reply, '(') + 1,
                        (INSTR (l_reply, ')')) - (INSTR (l_reply, '(')) - 1),
                ',',
                '.');
        --l_host  := SUBSTR(l_reply, 1, INSTR(l_reply, '.', 1, 4)-1);
        l_port1 :=
            TO_NUMBER (SUBSTR (l_reply,
                                 INSTR (l_reply,
                                        '.',
                                        1,
                                        4)
                               + 1,
                                 (  INSTR (l_reply,
                                           '.',
                                           1,
                                           5)
                                  - 1)
                               - (INSTR (l_reply,
                                         '.',
                                         1,
                                         4))));
        l_port2 :=
            TO_NUMBER (SUBSTR (l_reply,
                                 INSTR (l_reply,
                                        '.',
                                        1,
                                        5)
                               + 1));
        --l_conn := utl_tcp.open_connection(l_host, 256 * l_port1 + l_port2);
        l_conn :=
            UTL_TCP.open_connection (p_conn.remote_host,
                                     256 * l_port1 + l_port2);
        RETURN l_conn;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE LOGOUT (p_conn    IN OUT NOCOPY UTL_TCP.connection,
                      p_reply   IN            BOOLEAN := TRUE)
    AS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        send_command (p_conn, 'QUIT', p_reply);
        UTL_TCP.close_connection (p_conn);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_command (p_conn      IN OUT NOCOPY UTL_TCP.connection,
                            p_command   IN            VARCHAR2,
                            p_reply     IN            BOOLEAN := TRUE)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_result   PLS_INTEGER;
    BEGIN
        l_result := UTL_TCP.write_line (p_conn, p_command);

        -- If you get ORA-29260 after the PASV call, replace the above line with the following line.
        -- l_result := UTL_TCP.write_text(p_conn, p_command || utl_tcp.crlf, length(p_command || utl_tcp.crlf));
        IF p_reply
        THEN
            get_reply (p_conn);
        END IF;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get_reply (p_conn IN OUT NOCOPY UTL_TCP.connection)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_reply_code   VARCHAR2 (3) := NULL;
    BEGIN
        LOOP
            g_reply.EXTEND;
            g_reply (g_reply.LAST) := UTL_TCP.get_line (p_conn, TRUE);
            debug (g_reply (g_reply.LAST));

            IF l_reply_code IS NULL
            THEN
                l_reply_code := SUBSTR (g_reply (g_reply.LAST), 1, 3);
            END IF;

            IF SUBSTR (l_reply_code, 1, 1) IN ('4', '5')
            THEN
                raise_application_error (-20000, g_reply (g_reply.LAST));
            ELSIF (    SUBSTR (g_reply (g_reply.LAST), 1, 3) = l_reply_code
                   AND SUBSTR (g_reply (g_reply.LAST), 4, 1) = ' ')
            THEN
                EXIT;
            END IF;
        END LOOP;
    EXCEPTION
        WHEN UTL_TCP.end_of_input
        THEN
            NULL;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_local_ascii_data (p_dir IN VARCHAR2, p_file IN VARCHAR2)
        RETURN CLOB
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_bfile   BFILE;
        l_data    CLOB;
    BEGIN
        DBMS_LOB.createtemporary (lob_loc   => l_data,
                                  cache     => TRUE,
                                  dur       => DBMS_LOB.call);
        l_bfile := BFILENAME (p_dir, p_file);
        DBMS_LOB.fileopen (l_bfile, DBMS_LOB.file_readonly);

        IF DBMS_LOB.getlength (l_bfile) > 0
        THEN
            DBMS_LOB.loadfromfile (l_data,
                                   l_bfile,
                                   DBMS_LOB.getlength (l_bfile));
        END IF;

        DBMS_LOB.fileclose (l_bfile);
        RETURN l_data;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_local_binary_data (p_dir IN VARCHAR2, p_file IN VARCHAR2)
        RETURN BLOB
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_bfile   BFILE;
        l_data    BLOB;
    BEGIN
        DBMS_LOB.createtemporary (lob_loc   => l_data,
                                  cache     => TRUE,
                                  dur       => DBMS_LOB.call);
        l_bfile := BFILENAME (p_dir, p_file);
        DBMS_LOB.fileopen (l_bfile, DBMS_LOB.file_readonly);

        IF DBMS_LOB.getlength (l_bfile) > 0
        THEN
            DBMS_LOB.loadfromfile (l_data,
                                   l_bfile,
                                   DBMS_LOB.getlength (l_bfile));
        END IF;

        DBMS_LOB.fileclose (l_bfile);
        RETURN l_data;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_remote_ascii_data (
        p_conn   IN OUT NOCOPY UTL_TCP.connection,
        p_file   IN            VARCHAR2)
        RETURN CLOB
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn     UTL_TCP.connection;
        l_amount   PLS_INTEGER;
        l_buffer   VARCHAR2 (32767);
        l_data     CLOB;
    BEGIN
        DBMS_LOB.createtemporary (lob_loc   => l_data,
                                  cache     => TRUE,
                                  dur       => DBMS_LOB.call);
        l_conn := get_passive (p_conn);
        send_command (p_conn, 'TYPE A', TRUE);
        send_command (p_conn, 'RETR ' || p_file, TRUE);

        BEGIN
            LOOP
                l_amount := UTL_TCP.read_text (l_conn, l_buffer, 32767);
                DBMS_LOB.writeappend (l_data, l_amount, l_buffer);
            END LOOP;
        EXCEPTION
            WHEN UTL_TCP.end_of_input
            THEN
                NULL;
            WHEN OTHERS
            THEN
                NULL;
        END;

        UTL_TCP.close_connection (l_conn);
        get_reply (p_conn);
        RETURN l_data;
    EXCEPTION
        WHEN OTHERS
        THEN
            UTL_TCP.close_connection (l_conn);
            RAISE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_remote_binary_data (
        p_conn   IN OUT NOCOPY UTL_TCP.connection,
        p_file   IN            VARCHAR2)
        RETURN BLOB
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn     UTL_TCP.connection;
        l_amount   PLS_INTEGER;
        l_buffer   RAW (32767);
        l_data     BLOB;
    BEGIN
        DBMS_LOB.createtemporary (lob_loc   => l_data,
                                  cache     => TRUE,
                                  dur       => DBMS_LOB.call);
        l_conn := get_passive (p_conn);
        send_command (p_conn, 'RETR ' || p_file, TRUE);

        BEGIN
            LOOP
                l_amount := UTL_TCP.read_raw (l_conn, l_buffer, 32767);
                DBMS_LOB.writeappend (l_data, l_amount, l_buffer);
            END LOOP;
        EXCEPTION
            WHEN UTL_TCP.end_of_input
            THEN
                NULL;
            WHEN OTHERS
            THEN
                NULL;
        END;

        UTL_TCP.close_connection (l_conn);
        get_reply (p_conn);
        RETURN l_data;
    EXCEPTION
        WHEN OTHERS
        THEN
            UTL_TCP.close_connection (l_conn);
            RAISE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put_local_ascii_data (p_data   IN CLOB,
                                    p_dir    IN VARCHAR2,
                                    p_file   IN VARCHAR2)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_out_file   UTL_FILE.file_type;
        l_buffer     VARCHAR2 (32767);
        l_amount     BINARY_INTEGER := 32767;
        l_pos        INTEGER := 1;
        l_clob_len   INTEGER;
    BEGIN
        l_clob_len := DBMS_LOB.getlength (p_data);
        l_out_file :=
            UTL_FILE.fopen (p_dir,
                            p_file,
                            'w',
                            32767);

        WHILE l_pos <= l_clob_len
        LOOP
            DBMS_LOB.read (p_data,
                           l_amount,
                           l_pos,
                           l_buffer);

            IF g_convert_crlf
            THEN
                l_buffer := REPLACE (l_buffer, CHR (13), NULL);
            END IF;

            UTL_FILE.put (l_out_file, l_buffer);
            UTL_FILE.fflush (l_out_file);
            l_pos := l_pos + l_amount;
        END LOOP;

        UTL_FILE.fclose (l_out_file);
    EXCEPTION
        WHEN OTHERS
        THEN
            IF UTL_FILE.is_open (l_out_file)
            THEN
                UTL_FILE.fclose (l_out_file);
            END IF;

            RAISE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put_local_binary_data (p_data   IN BLOB,
                                     p_dir    IN VARCHAR2,
                                     p_file   IN VARCHAR2)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_out_file   UTL_FILE.file_type;
        l_buffer     RAW (32767);
        l_amount     BINARY_INTEGER := 32767;
        l_pos        INTEGER := 1;
        l_blob_len   INTEGER;
    BEGIN
        l_blob_len := DBMS_LOB.getlength (p_data);
        l_out_file :=
            UTL_FILE.fopen (p_dir,
                            p_file,
                            'wb',
                            32767);

        WHILE l_pos <= l_blob_len
        LOOP
            DBMS_LOB.read (p_data,
                           l_amount,
                           l_pos,
                           l_buffer);
            UTL_FILE.put_raw (l_out_file, l_buffer, TRUE);
            UTL_FILE.fflush (l_out_file);
            l_pos := l_pos + l_amount;
        END LOOP;

        UTL_FILE.fclose (l_out_file);
    EXCEPTION
        WHEN OTHERS
        THEN
            IF UTL_FILE.is_open (l_out_file)
            THEN
                UTL_FILE.fclose (l_out_file);
            END IF;

            RAISE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put_remote_ascii_data (
        p_conn   IN OUT NOCOPY UTL_TCP.connection,
        p_file   IN            VARCHAR2,
        p_data   IN            CLOB)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn       UTL_TCP.connection;
        l_result     PLS_INTEGER;
        l_buffer     VARCHAR2 (32767);
        l_amount     BINARY_INTEGER := 32767;
        -- Switch to 10000 (or use binary) if you get ORA-06502 from this line.
        l_pos        INTEGER := 1;
        l_clob_len   INTEGER;
    BEGIN
        l_conn := get_passive (p_conn);

        send_command (p_conn, 'TYPE A', TRUE);
        send_command (p_conn, 'STOR ' || p_file, TRUE);
        l_clob_len := DBMS_LOB.getlength (p_data);

        WHILE l_pos <= l_clob_len
        LOOP
            DBMS_LOB.read (p_data,
                           l_amount,
                           l_pos,
                           l_buffer);

            IF g_convert_crlf
            THEN
                l_buffer := REPLACE (l_buffer, CHR (13), NULL);
            END IF;

            l_result :=
                UTL_TCP.write_text (l_conn, l_buffer, LENGTH (l_buffer));
            UTL_TCP.flush (l_conn);
            l_pos := l_pos + l_amount;
        END LOOP;

        UTL_TCP.close_connection (l_conn);
    -- The following line allows some people to make multiple calls from one connection.
    -- It causes the operation to hang for me, hence it is commented out by default.
    -- get_reply(p_conn);
    EXCEPTION
        WHEN OTHERS
        THEN
            UTL_TCP.close_connection (l_conn);
            RAISE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put_remote_binary_data (
        p_conn   IN OUT NOCOPY UTL_TCP.connection,
        p_file   IN            VARCHAR2,
        p_data   IN            BLOB)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn       UTL_TCP.connection;
        l_result     PLS_INTEGER;
        l_buffer     RAW (32767);
        l_amount     BINARY_INTEGER := 32767;
        l_pos        INTEGER := 1;
        l_blob_len   INTEGER;
    BEGIN
        l_conn := get_passive (p_conn);

        --setting binary type
        send_command (p_conn, 'TYPE I', TRUE);
        send_command (p_conn, 'STOR ' || p_file, TRUE);
        l_blob_len := DBMS_LOB.getlength (p_data);

        WHILE l_pos <= l_blob_len
        LOOP
            DBMS_LOB.read (p_data,
                           l_amount,
                           l_pos,
                           l_buffer);
            l_result := UTL_TCP.write_raw (l_conn, l_buffer, l_amount);
            UTL_TCP.flush (l_conn);
            l_pos := l_pos + l_amount;
        END LOOP;

        UTL_TCP.close_connection (l_conn);
    -- The following line allows some people to make multiple calls from one connection.
    -- It causes the operation to hang for me, hence it is commented out by default.
    -- get_reply(p_conn);
    EXCEPTION
        WHEN OTHERS
        THEN
            UTL_TCP.close_connection (l_conn);
            RAISE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get (p_conn        IN OUT NOCOPY UTL_TCP.connection,
                   p_from_file   IN            VARCHAR2,
                   p_to_dir      IN            VARCHAR2,
                   p_to_file     IN            VARCHAR2)
    AS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        IF g_binary
        THEN
            put_local_binary_data (
                p_data   => get_remote_binary_data (p_conn, p_from_file),
                p_dir    => p_to_dir,
                p_file   => p_to_file);
        ELSE
            put_local_ascii_data (
                p_data   => get_remote_ascii_data (p_conn, p_from_file),
                p_dir    => p_to_dir,
                p_file   => p_to_file);
        END IF;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put (p_conn        IN OUT NOCOPY UTL_TCP.connection,
                   p_from_dir    IN            VARCHAR2,
                   p_from_file   IN            VARCHAR2,
                   p_to_file     IN            VARCHAR2)
    AS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        IF g_binary
        THEN
            put_remote_binary_data (
                p_conn   => p_conn,
                p_file   => p_to_file,
                p_data   => get_local_binary_data (p_from_dir, p_from_file));
        ELSE
            put_remote_ascii_data (
                p_conn   => p_conn,
                p_file   => p_to_file,
                p_data   => get_local_ascii_data (p_from_dir, p_from_file));
        END IF;

        get_reply (p_conn);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get_direct (p_conn        IN OUT NOCOPY UTL_TCP.connection,
                          p_from_file   IN            VARCHAR2,
                          p_to_dir      IN            VARCHAR2,
                          p_to_file     IN            VARCHAR2)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn         UTL_TCP.connection;
        l_out_file     UTL_FILE.file_type;
        l_amount       PLS_INTEGER;
        l_buffer       VARCHAR2 (32767);
        l_raw_buffer   RAW (32767);
    BEGIN
        l_conn := get_passive (p_conn);
        send_command (p_conn, 'RETR ' || p_from_file, TRUE);

        IF g_binary
        THEN
            l_out_file :=
                UTL_FILE.fopen (p_to_dir,
                                p_to_file,
                                'wb',
                                32767);
        ELSE
            l_out_file :=
                UTL_FILE.fopen (p_to_dir,
                                p_to_file,
                                'w',
                                32767);
        END IF;

        BEGIN
            LOOP
                IF g_binary
                THEN
                    l_amount := UTL_TCP.read_raw (l_conn, l_raw_buffer, 32767);
                    UTL_FILE.put_raw (l_out_file, l_raw_buffer, TRUE);
                ELSE
                    l_amount := UTL_TCP.read_text (l_conn, l_buffer, 32767);

                    IF g_convert_crlf
                    THEN
                        l_buffer := REPLACE (l_buffer, CHR (13), NULL);
                    END IF;

                    UTL_FILE.put (l_out_file, l_buffer);
                END IF;

                UTL_FILE.fflush (l_out_file);
            END LOOP;
        EXCEPTION
            WHEN UTL_TCP.end_of_input
            THEN
                NULL;
            WHEN OTHERS
            THEN
                NULL;
        END;

        UTL_FILE.fclose (l_out_file);
        UTL_TCP.close_connection (l_conn);
    EXCEPTION
        WHEN OTHERS
        THEN
            IF UTL_FILE.is_open (l_out_file)
            THEN
                UTL_FILE.fclose (l_out_file);
            END IF;

            RAISE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put_direct (p_conn        IN OUT NOCOPY UTL_TCP.connection,
                          p_from_dir    IN            VARCHAR2,
                          p_from_file   IN            VARCHAR2,
                          p_to_file     IN            VARCHAR2)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn         UTL_TCP.connection;
        l_bfile        BFILE;
        l_result       PLS_INTEGER;
        l_amount       PLS_INTEGER := 32767;
        l_raw_buffer   RAW (32767);
        l_len          NUMBER;
        l_pos          NUMBER := 1;
        ex_ascii       EXCEPTION;
    BEGIN
        IF NOT g_binary
        THEN
            RAISE ex_ascii;
        END IF;

        l_conn := get_passive (p_conn);
        send_command (p_conn, 'STOR ' || p_to_file, TRUE);
        l_bfile := BFILENAME (p_from_dir, p_from_file);
        DBMS_LOB.fileopen (l_bfile, DBMS_LOB.file_readonly);
        l_len := DBMS_LOB.getlength (l_bfile);

        WHILE l_pos <= l_len
        LOOP
            DBMS_LOB.read (l_bfile,
                           l_amount,
                           l_pos,
                           l_raw_buffer);
            debug (l_amount);
            l_result := UTL_TCP.write_raw (l_conn, l_raw_buffer, l_amount);
            l_pos := l_pos + l_amount;
        END LOOP;

        DBMS_LOB.fileclose (l_bfile);
        UTL_TCP.close_connection (l_conn);
    EXCEPTION
        WHEN ex_ascii
        THEN
            raise_application_error (
                -20000,
                'PUT_DIRECT not available in ASCII mode.');
        WHEN OTHERS
        THEN
            IF DBMS_LOB.fileisopen (l_bfile) = 1
            THEN
                DBMS_LOB.fileclose (l_bfile);
            END IF;

            RAISE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE HELP (p_conn IN OUT NOCOPY UTL_TCP.connection)
    AS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        send_command (p_conn, 'HELP', TRUE);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE ASCII (p_conn IN OUT NOCOPY UTL_TCP.connection)
    AS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        send_command (p_conn, 'TYPE A', TRUE);
        g_binary := FALSE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE binary (p_conn IN OUT NOCOPY UTL_TCP.connection)
    AS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        send_command (p_conn, 'TYPE I', TRUE);
        g_binary := TRUE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE list (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                    p_dir    IN            VARCHAR2,
                    p_list      OUT        t_string_table)
    AS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn         UTL_TCP.connection;
        l_list         t_string_table := t_string_table ();
        l_reply_code   VARCHAR2 (3) := NULL;
    BEGIN
        l_conn := get_passive (p_conn);
        send_command (p_conn, 'LIST ' || p_dir, TRUE);

        BEGIN
            LOOP
                l_list.EXTEND;
                l_list (l_list.LAST) := UTL_TCP.get_line (l_conn, TRUE);
                debug (l_list (l_list.LAST));

                IF l_reply_code IS NULL
                THEN
                    l_reply_code := SUBSTR (l_list (l_list.LAST), 1, 3);
                END IF;

                IF (    SUBSTR (l_reply_code, 1, 1) IN ('4', '5')
                    AND SUBSTR (l_reply_code, 4, 1) = ' ')
                THEN
                    raise_application_error (-20000, l_list (l_list.LAST));
                ELSIF (    SUBSTR (g_reply (g_reply.LAST), 1, 3) =
                               l_reply_code
                       AND SUBSTR (g_reply (g_reply.LAST), 4, 1) = ' ')
                THEN
                    EXIT;
                END IF;
            END LOOP;
        EXCEPTION
            WHEN UTL_TCP.end_of_input
            THEN
                NULL;
        END;

        l_list.delete (l_list.LAST);
        p_list := l_list;
        UTL_TCP.close_connection (l_conn);
        get_reply (p_conn);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE nlst (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                    p_dir    IN            VARCHAR2,
                    p_list      OUT        t_string_table)
    AS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn         UTL_TCP.connection;
        l_list         t_string_table := t_string_table ();
        l_reply_code   VARCHAR2 (3) := NULL;
    BEGIN
        l_conn := get_passive (p_conn);
        send_command (p_conn, 'NLST ' || p_dir, TRUE);

        BEGIN
            LOOP
                l_list.EXTEND;
                l_list (l_list.LAST) := UTL_TCP.get_line (l_conn, TRUE);
                debug (l_list (l_list.LAST));

                IF l_reply_code IS NULL
                THEN
                    l_reply_code := SUBSTR (l_list (l_list.LAST), 1, 3);
                END IF;

                IF (    SUBSTR (l_reply_code, 1, 1) IN ('4', '5')
                    AND SUBSTR (l_reply_code, 4, 1) = ' ')
                THEN
                    raise_application_error (-20000, l_list (l_list.LAST));
                ELSIF (    SUBSTR (g_reply (g_reply.LAST), 1, 3) =
                               l_reply_code
                       AND SUBSTR (g_reply (g_reply.LAST), 4, 1) = ' ')
                THEN
                    EXIT;
                END IF;
            END LOOP;
        EXCEPTION
            WHEN UTL_TCP.end_of_input
            THEN
                NULL;
        END;

        l_list.delete (l_list.LAST);
        p_list := l_list;
        UTL_TCP.close_connection (l_conn);
        get_reply (p_conn);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE rename (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                      p_from   IN            VARCHAR2,
                      p_to     IN            VARCHAR2)
    AS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn   UTL_TCP.connection;
    BEGIN
        l_conn := get_passive (p_conn);
        send_command (p_conn, 'RNFR ' || p_from, TRUE);
        send_command (p_conn, 'RNTO ' || p_to, TRUE);
        LOGOUT (l_conn, FALSE);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE delete (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                      p_file   IN            VARCHAR2)
    AS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn   UTL_TCP.connection;
    BEGIN
        l_conn := get_passive (p_conn);
        send_command (p_conn, 'DELE ' || p_file, TRUE);
        LOGOUT (l_conn, FALSE);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE mkdir (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                     p_dir    IN            VARCHAR2)
    AS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn   UTL_TCP.connection;
    BEGIN
        l_conn := get_passive (p_conn);
        send_command (p_conn, 'MKD ' || p_dir, TRUE);
        LOGOUT (l_conn, FALSE);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE rmdir (p_conn   IN OUT NOCOPY UTL_TCP.connection,
                     p_dir    IN            VARCHAR2)
    AS
        ------------------------------------------------------------------------------------------------------------------------
        l_conn   UTL_TCP.connection;
    BEGIN
        l_conn := get_passive (p_conn);
        send_command (p_conn, 'RMD ' || p_dir, TRUE);
        LOGOUT (l_conn, FALSE);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE convert_crlf (p_status IN BOOLEAN)
    AS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        g_convert_crlf := p_status;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE debug (p_text IN VARCHAR2)
    IS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        IF g_debug
        THEN
            DBMS_OUTPUT.put_line (SUBSTR (p_text, 1, 255));
        END IF;
    END;
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE PACKAGE jg_ftp_configuration
IS
    ------------------------------------------------------------------------------------------------------------------------

    sf_ftp_host                 VARCHAR2 (30) := '193.202.117.201';
    sf_ftp_user                 VARCHAR2 (30) := 'jbs';
    sf_ftp_password             VARCHAR2 (30) := 'p6ucuyUk';
    sf_ftp_port                 PLS_INTEGER := 21;

    sf_ftp_in_folder            VARCHAR2 (30) := 'IN';
    sf_ftp_out_folder           VARCHAR2 (30) := 'OUT';
    sf_ftp_out_archive_folder   VARCHAR2 (30) := 'OUT/Archive';
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE PACKAGE jg_input_sync
IS
    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE process_all;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get_from_ftp;
END;
/

CREATE OR REPLACE PACKAGE BODY jg_input_sync
IS
    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_query_from_sql_repository (
        p_object_type   IN jg_input_log.object_type%TYPE)
        RETURN jg_sql_repository.sql_query%TYPE
    IS
        ------------------------------------------------------------------------------------------------------------------------
        CURSOR c_sql_query (
            pc_object_type    jg_sql_repository.object_type%TYPE)
        IS
            SELECT sql_query
              FROM jg_sql_repository
             WHERE object_type = pc_object_type;

        v_sql_query   jg_sql_repository.sql_query%TYPE;
    BEGIN
        OPEN c_sql_query (p_object_type);

        FETCH c_sql_query   INTO v_sql_query;

        CLOSE c_sql_query;

        IF v_sql_query IS NULL
        THEN
            assert (
                FALSE,
                   'Brak zdefiniowanego zapytania dla obiektu o typie '''
                || p_object_type
                || '');
        END IF;

        RETURN v_sql_query;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_xslt_from_repository (
        p_object_type   IN jg_sql_repository.object_type%TYPE)
        RETURN jg_sql_repository.xslt%TYPE
    IS
        ------------------------------------------------------------------------------------------------------------------------
        CURSOR c_xslt (pc_object_type jg_sql_repository.object_type%TYPE)
        IS
            SELECT xslt
              FROM jg_sql_repository
             WHERE object_type = pc_object_type;

        v_xslt   jg_sql_repository.xslt%TYPE;
    BEGIN
        OPEN c_xslt (p_object_type);

        FETCH c_xslt   INTO v_xslt;

        CLOSE c_xslt;

        IF v_xslt IS NULL
        THEN
            assert (
                FALSE,
                   'Brak zdefiniowanego szablonu xslt dla obiektu o typie '''
                || p_object_type
                || '');
        END IF;

        RETURN v_xslt;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION create_xml (
        p_sql_query     IN jg_sql_repository.sql_query%TYPE,
        p_object_type   IN jg_sql_repository.object_type%TYPE)
        RETURN CLOB
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_ctx              DBMS_XMLSAVE.ctxtype;
        v_xml              CLOB;
        r_current_format   pa_xmltype.tr_format;
    BEGIN
        r_current_format := pa_xmltype.biezacy_format;
        pa_xmltype.ustaw_format_xml ();

        v_ctx := DBMS_XMLGEN.newcontext (querystring => p_sql_query);
        DBMS_XMLGEN.setrowsettag (v_ctx, NULL);
        DBMS_XMLGEN.setrowtag (v_ctx, p_object_type);
        v_xml := DBMS_XMLGEN.getxml (v_ctx);
        DBMS_XMLGEN.closecontext (v_ctx);

        pa_xmltype.ustaw_format (r_current_format);
        RETURN v_xml;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION transform_xml (
        p_xml           IN CLOB,
        p_object_type   IN jg_sql_repository.object_type%TYPE,
        p_xslt          IN CLOB DEFAULT NULL)
        RETURN XMLTYPE
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_xslt             jg_sql_repository.xslt%TYPE := p_xslt;
        v_xml              XMLTYPE;
        r_current_format   pa_xmltype.tr_format;
        v_result           XMLTYPE;
    BEGIN
        r_current_format := pa_xmltype.biezacy_format;
        pa_xmltype.ustaw_format_xml ();

        IF v_xslt IS NULL
        THEN
            v_xslt := get_xslt_from_repository (p_object_type => p_object_type);
        END IF;

        v_xml := xmltype.createxml (p_xml);
        v_result := v_xml.transform (xmltype (v_xslt));

        pa_xmltype.ustaw_format (r_current_format);
        RETURN v_result;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_result (
        p_inlo_id     IN jg_input_log.id%TYPE,
        p_status      IN jg_input_log.status%TYPE,
        p_object_id   IN jg_input_log.object_id%TYPE,
        p_error       IN jg_input_log.error%TYPE DEFAULT NULL)
    IS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        UPDATE jg_input_log
           SET status = p_status,
               processed_date = SYSDATE,
               object_id = p_object_id,
               error = p_error
         WHERE id = p_inlo_id;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION import_customer (
        p_xml           IN CLOB,
        p_object_type   IN jg_sql_repository.object_type%TYPE)
        RETURN jg_input_log.object_id%TYPE
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_xml                XMLTYPE;
        v_core_ns   CONSTANT VARCHAR2 (200)
            := 'xmlns="http://www.teta.com.pl/teta2000/kontrahent-1"' ;
    BEGIN
        v_xml := transform_xml (p_xml => p_xml, p_object_type => p_object_type);
        apix_lg_konr.update_obj (p_konr                           => v_xml.getclobval,
                                 p_update_limit                   => FALSE,
                                 p_update_addresses_by_konr_mdf   => TRUE);

        RETURN lg_konr_sql.id (
                   p_symbol   => pa_xmltype.wartosc (
                                    v_xml,
                                    '/PA_KONTRAHENT_TK/SYMBOL',
                                    v_core_ns));
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION to_money (p_value VARCHAR2)
        RETURN NUMBER
    IS
        v_value   NUMBER (10, 2);
    BEGIN
        SELECT TO_NUMBER (REGEXP_REPLACE (p_value, '[,.]', TRIM (VALUE)))
          INTO v_value
          FROM nls_session_parameters
         WHERE parameter = 'NLS_NUMERIC_CHARACTERS';

        RETURN ROUND (v_value, 2);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION import_cash_receipts (
        p_operation_id   IN jg_input_log.id%TYPE,
        p_object_type    IN jg_sql_repository.object_type%TYPE)
        RETURN jg_input_log.object_id%TYPE
    IS
        r_ksks        rk_ks_kasy%ROWTYPE;
        v_konr_id     ap_kontrahenci.id%TYPE;
        v_ksrk_guid   rk_ks_raporty_kasowe.guid%TYPE;
        vr_document   api_rk_ks_ksdk.tr_document;
        vr_payment    api_rk_ks_ksdk.tr_payment;
        v_ksdk_guid   rk_ks_dokumenty_kasowe.guid%TYPE;
        v_ksdk_id     rk_ks_dokumenty_kasowe.id%TYPE;
        v_dosp_id     lg_sal_invoices.id%TYPE;
        r_plat        lg_dosp_platnosci%ROWTYPE;
        r_ksrk        rk_ks_raporty_kasowe%ROWTYPE;
    BEGIN
        FOR r_ksdk
            IN (SELECT cash_receipt.*
                FROM jg_input_log LOG,
                     XMLTABLE (
                         '//NewKPConfirmation'
                         PASSING xmltype (LOG.xml)
                         COLUMNS description  VARCHAR2 (200)
                                     PATH '/NewKPConfirmation/PaymentTitle',
                                 cash_receipt_date  VARCHAR2 (30)
                                     PATH '/NewKPConfirmation/DateOfCashCollection',
                                 cash_paid  VARCHAR2 (30)
                                     PATH '/NewKPConfirmation/CollectedAmountTotal',
                                 konr_symbol  VARCHAR2 (30)
                                     PATH '/NewKPConfirmation/CustomerID',
                                 cash_register_symbol  VARCHAR2 (100)
                                     PATH '/NewKPConfirmation/BillingAccountNumber',
                                 cash_receipt_number_1  VARCHAR2 (100)
                                     PATH '/NewKPConfirmation/KPNumber',
                                 cash_receipt_number_2  VARCHAR2 (100)
                                     PATH '/NewKPConfirmation/ExternalKPNumber')
                     cash_receipt
                WHERE LOG.id = p_operation_id)
        LOOP
            IF NOT rk_kska_sql.exists_by_symbol (
                       p_symbol   => r_ksdk.cash_register_symbol)
            THEN
                pa_bledy.wywolaj_bld (
                    p_nr_bledu      => -20001,
                    p_tekst_bledu   =>    'Nie istnieje kasa o symbolu: '
                                       || r_ksdk.cash_register_symbol);
            END IF;


            r_ksks :=
                rk_kska_sql.rt (
                    p_id      => rk_kska_sql.id_by_symbol (
                                    p_symbol   => r_ksdk.cash_register_symbol),
                    p_raise   => FALSE);

            IF r_ksdk.konr_symbol IS NOT NULL
            THEN
                IF NOT lg_konr_sql.istnieje (p_symbol => r_ksdk.konr_symbol)
                THEN
                    pa_bledy.wywolaj_bld (
                        p_nr_bledu      => -20001,
                        p_tekst_bledu   =>    'Nie istnieje kontrahent o symbolu: '
                                           || r_ksdk.konr_symbol);
                END IF;

                v_konr_id :=
                    lg_konr_sql.id_uk1 (p_symbol => r_ksdk.konr_symbol);
            END IF;

            FOR r_exists IN (SELECT symbol_dokumentu
                             FROM rk_ks_dokumenty_kasowe
                             WHERE t_02 = r_ksdk.cash_receipt_number_2)
            LOOP
                pa_bledy.wywolaj_bld (
                    p_nr_bledu      => -20001,
                    p_tekst_bledu   =>    'Dokument o symbolu: '
                                       || r_ksdk.cash_receipt_number_2
                                       || ' znajduje siÍ juø w kasie. Otrzyma≥ symbol: '
                                       || r_exists.symbol_dokumentu);
            END LOOP;



            vr_document.konr_id := v_konr_id;
            vr_document.cash_paid := to_money (r_ksdk.cash_paid);
            vr_document.date :=
                TRUNC (
                    TO_DATE (REPLACE (r_ksdk.cash_receipt_date, 'T', ' '),
                             'YYYY-MM-DD hh24:mi:ss'),
                    'DD');
            vr_document.description := r_ksdk.description;
            v_ksrk_guid :=
                api_rk_ks_ksrk.current_cash_report (p_kska_id    => r_ksks.id,
                                                    p_currency   => 'PLN');


            IF v_ksrk_guid IS NOT NULL
            THEN
                r_ksrk :=
                    rk_ksrk_sql.rt (rk_ksrk_sql.id_by_guid (v_ksrk_guid));

                IF r_ksrk.data_do < TRUNC (vr_document.date)
                THEN
                    api_rk_ks_ksrk.close_cash_report (p_guid => v_ksrk_guid);
                    v_ksrk_guid :=
                        api_rk_ks_ksrk.open_cash_report (
                            p_kska_id     => r_ksks.id,
                            p_currency    => 'PLN',
                            p_date_from   => TRUNC (vr_document.date),
                            p_date_to     => TRUNC (vr_document.date));
                END IF;
            ELSE
                v_ksrk_guid :=
                    api_rk_ks_ksrk.open_cash_report (
                        p_kska_id     => r_ksks.id,
                        p_currency    => 'PLN',
                        p_date_from   => TRUNC (vr_document.date),
                        p_date_to     => TRUNC (vr_document.date));
            END IF;

            v_ksdk_guid :=
                api_rk_ks_ksdk.create_document (p_ksrk_guid   => v_ksrk_guid,
                                                pr_document   => vr_document);



            FOR r_payments
                IN (SELECT cash_payments.*
                    FROM jg_input_log LOG,
                         XMLTABLE (
                             '//NewKPConfirmation/Items/Item'
                             PASSING xmltype (LOG.xml)
                             COLUMNS payments_no VARCHAR2 (200) PATH '/Item/ItemNumber',
                                     paid_amount VARCHAR2 (30) PATH '/Item/CollectedAmount',
                                     invoice_symbol VARCHAR2 (30) PATH '/Item/InvoiceNumber')
                         cash_payments
                    WHERE LOG.id = p_operation_id)
            LOOP
                IF     r_payments.invoice_symbol IS NOT NULL
                   AND lg_dosp_sql.istnieje (
                           p_symbol   => r_payments.invoice_symbol)
                THEN
                    v_dosp_id :=
                        lg_dosp_sql.id (p_symbol => r_payments.invoice_symbol);

                    r_plat :=
                        lg_dosp_plat_sql.rt (
                            p_id   => lg_dosp_plat_sql.id_pierwszej_platnosci (
                                         p_dosp_id   => v_dosp_id));

                    vr_payment := NULL;

                    vr_payment.symbol := r_plat.symbol_platnosci;
                    vr_payment.guid := r_plat.guid;
                    vr_payment.date := r_plat.data_platnosci;
                    vr_payment.form := r_plat.foza_kod;
                    vr_payment.paid_amount :=
                        to_money (r_payments.paid_amount);

                    api_rk_ks_ksdk.create_payment (
                        p_ksdk_guid   => v_ksdk_guid,
                        pr_payment    => vr_payment);
                ELSE
                    vr_payment := NULL;


                    vr_payment.symbol := r_payments.invoice_symbol;
                    vr_payment.paid_amount :=
                        to_money (r_payments.paid_amount);
                    api_rk_ks_ksdk.create_payment (
                        p_ksdk_guid   => v_ksdk_guid,
                        pr_payment    => vr_payment);
                END IF;
            END LOOP;

            UPDATE rk_ks_dokumenty_kasowe
               SET t_01 = r_ksdk.cash_receipt_number_1,
                   t_02 = r_ksdk.cash_receipt_number_2
             WHERE guid = v_ksdk_guid;

            api_rk_ks_ksdk.approve_document (p_ksdk_guid => v_ksdk_guid);
            v_ksdk_id := rk_ksdk_sql.id_dla_guid (p_guid => v_ksdk_guid);
        END LOOP;

        RETURN v_ksdk_id;
    END;

    ------------------------------------------------------------------------------------------------------------------------

    FUNCTION import_sale_order (
        p_operation_id   IN jg_output_log.id%TYPE,
        p_object_type    IN jg_sql_repository.object_type%TYPE)
        RETURN jg_input_log.object_id%TYPE
    IS
        ------------------------------------------------------------------------------------------------------------------------
        CURSOR c_sord (pc_doc_symbol_rcv lg_sal_orders.doc_symbol_rcv%TYPE)
        IS
            SELECT symbol
              FROM lg_sal_orders sord
             WHERE sord.doc_symbol_rcv = pc_doc_symbol_rcv;

        v_xml                XMLTYPE;
        v_xml_clob           CLOB;
        v_sql_query          CLOB;
        v_symbol             lg_sal_orders.symbol%TYPE;
        v_cinn_id            lg_sal_orders.cinn_id%TYPE;
        v_data_realizacji    lg_sal_orders.realization_date%TYPE;
        v_numer              NUMBER;
        v_wzrc_id            lg_documents_templates.id%TYPE;
        v_sord_id            lg_sal_orders.id%TYPE;
        v_order_type         VARCHAR2 (1);
        v_doc_symbol_rcv     lg_sal_orders.doc_symbol_rcv%TYPE;
        v_should_calculate   BOOLEAN := FALSE;
    BEGIN
        pa_wass_def.ustaw (p_nazwa => 'IMPORT_INFINITE', p_wartosc => 'T');

        v_sql_query := get_query_from_sql_repository (p_object_type);
        v_sql_query :=
            REPLACE (v_sql_query, ':p_operation_id', p_operation_id);

        v_xml_clob := create_xml (v_sql_query, p_object_type);
        v_order_type :=
            pa_xmltype.wartosc (xmltype (v_xml_clob), '/ORDER/ORDER_TYPE');
        v_doc_symbol_rcv :=
            pa_xmltype.wartosc (xmltype (v_xml_clob), '/ORDER/ORDER_NUMBER');

        v_xml :=
            transform_xml (p_xml => v_xml_clob, p_object_type => p_object_type);

        v_wzrc_id :=
            lg_wzrc_sql.id (
                p_wzorzec   => pa_xmltype.wartosc (v_xml,
                                                   '/LG_ZASP_T/WZORZEC'));
        v_data_realizacji :=
            TO_DATE (
                pa_xmltype.wartosc (v_xml, '/LG_ZASP_T/DATA_REALIZACJI'),
                'YYYY-MM-DD"T"HH24:MI:SS".0000000+02:00"');

        OPEN c_sord (v_doc_symbol_rcv);

        FETCH c_sord   INTO v_symbol;

        IF c_sord%NOTFOUND
        THEN
            v_should_calculate := TRUE;

            lg_dosp_numerowanie.ustal_kolejny_numer (
                po_symbol          => v_symbol,
                po_cinn_id         => v_cinn_id,
                po_numer           => v_numer,
                p_data_faktury     => v_data_realizacji,
                p_data_sprzedazy   => v_data_realizacji,
                p_wzrc_id          => v_wzrc_id);
        END IF;

        CLOSE c_sord;

        v_xml :=
            xmltype.APPENDCHILDXML (
                v_xml,
                'LG_ZASP_T',
                xmltype (
                    '<SYMBOL_DOKUMENTU>' || v_symbol || '</SYMBOL_DOKUMENTU>'));
        apix_lg_zasp.aktualizuj (p_zamowienie => v_xml.getclobval);
        v_sord_id := lg_sord_sql.id_symbol (p_symbol => v_symbol);

        IF v_should_calculate
        THEN
            FOR r_dosi IN (SELECT *
                           FROM lg_sal_orders_it sori
                           WHERE sori.document_id = v_sord_id)
            LOOP
                lg_dosi_def.przelicz_wartosci_na_dosi (
                    po_cena                        => r_dosi.net_price,
                    po_wartosc_brutto              => r_dosi.gross_value,
                    po_wartosc_netto               => r_dosi.net_value,
                    po_wartosc_vat                 => r_dosi.vat_value,
                    p_cena_z_cennika               => pa_liczba.jezeli (
                                                         r_dosi.doc_pricing_type = 'N',
                                                         r_dosi.price_from_list_n,
                                                         r_dosi.price_from_list_g),
                    p_ilosc                        => r_dosi.quantity,
                    p_stva_id                      => r_dosi.stva_id,
                    p_upust_cj_z_global            => pa_liczba.jezeli (
                                                         r_dosi.doc_pricing_type = 'N',
                                                         r_dosi.discount_unit_price_glb_n,
                                                         r_dosi.discount_unit_price_glb_g),
                    p_upust_cj_z_pozycji           => pa_liczba.jezeli (
                                                         r_dosi.doc_pricing_type   = 'N',
                                                         r_dosi.discount_unit_price_line_n,
                                                         r_dosi.discount_unit_price_line_g),
                    p_obciazenie_zwolnienie_ceny   => r_dosi.price_difference,
                    p_wg_cen                       => r_dosi.doc_pricing_type,
                    p_typ_faktury                  => r_dosi.doc_type);

                lg_sori_def.update_row (pr_this => r_dosi);
            END LOOP;
        END IF;

        lg_dosp_obe.zakoncz;
        pa_wass_def.usun (p_nazwa => 'IMPORT_INFINITE');

        IF lg_sord_agd.global_discount (p_id => v_sord_id) != 0
        THEN
            lg_dosp_def.zmien_dolaczono_upust_glb (
                p_dosp_id               => v_sord_id,
                p_dolaczono_upust_glb   => 'T');
        END IF;

        IF v_order_type IN ('O')
        THEN
            UPDATE lg_sal_orders
               SET generate_warehouse_doc = 'T'
             WHERE id = v_sord_id;

            lg_dosp_def.zatwierdz_dosp (p_dosp_id => v_sord_id);
        END IF;

        RETURN v_sord_id;
    END;

    ------------------------------------------------------------------------------------------------------------------------

    PROCEDURE send_response
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_xml              XMLTYPE;
        v_xml_clob         CLOB;
        v_xslt             CLOB;
        v_sql_query        VARCHAR2 (4000);
        v_oryginal_id      VARCHAR2 (100);
        v_sciezka          VARCHAR2 (500);
        v_ctx              DBMS_XMLSAVE.ctxtype;
        r_current_format   pa_xmltype.tr_format;
        v_xml_type         XMLTYPE;
        v_xml_response     CLOB;
    BEGIN
        FOR r_inlo IN (SELECT *
                       FROM jg_input_log inlo
                       WHERE LENGTH (inlo.xml_response) = 0)
        LOOP
            IF r_inlo.object_type = 'ORDER'
            THEN
                v_xml_response := NULL;

                IF r_inlo.object_id IS NOT NULL
                THEN
                    v_sql_query :=
                           'SELECT sord.doc_symbol_rcv order_number,
                               ''ESTABLISHED''     status,
                               CURSOR (SELECT to_char(zare.data_modyfikacji,''YYYY/MM/DD HH24:MI:SS'')             reservation_date,
                                              sori.item_symbol                  commodity_id,
                                              sori.quantity                     quantity_ordered,
                                              NVL(reze.ilosc_zarezerwowana, 0)  quantity_reserved
                                         FROM lg_sal_orders_it sori,
                                              lg_rzm_zadania_rezerwacji zare,
                                              lg_rzm_rezerwacje reze
                                        WHERE     zare.zrre_id(+) = sori.id
                                              AND reze.zare_id(+) = zare.id
                                              AND sori.document_id = sord.id) reservations
                          FROM lg_sal_orders sord
                         WHERE sord.id = '
                        || r_inlo.object_id;

                    v_xslt :=
                        '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                           <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                           <xsl:strip-space elements="*"/>
                           <xsl:template match="node()|@*">
                             <xsl:copy>
                               <xsl:apply-templates select="node()|@*"/>
                             </xsl:copy>
                          </xsl:template>
                          <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                          <xsl:template priority="2" match="RESERVATIONS_ROW">
                            <RESERVATION><xsl:apply-templates /></RESERVATION>
                          </xsl:template>
                          </xsl:stylesheet>';

                    v_xml_clob := create_xml (v_sql_query, 'ORDER_RESPONSE');


                    IF v_xml_clob IS NOT NULL
                    THEN
                        v_xml :=
                            transform_xml (v_xml_clob,
                                           'ORDER_RESPONSE',
                                           v_xslt);

                        jg_output_sync.send_text_file_to_ftp (
                            p_xml         => v_xml.getclobval (),
                            p_file_name   =>    '/IN/responses/orders/orderExtended_'
                                             || r_inlo.file_name);

                        v_xml_response := v_xml.getclobval ();
                    END IF;
                END IF;

                v_sciezka := '/Order/OrderHeader/OrderNumber';

                BEGIN
                    v_oryginal_id :=
                        pa_xmltype.wartosc (px_xml      => xmltype (r_inlo.xml),
                                            p_sciezka   => v_sciezka);
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        v_oryginal_id := 'TO_CHAR(NULL)';
                END;

                v_sql_query :=
                       'SELECT '
                    || v_oryginal_id
                    || ' order_number,
                               status,
                               TO_CHAR(processed_date,''YYYY-MM-DD HH24:MI:SS'') processed_date,
                               TO_CHAR(log_date,''YYYY-MM-DD HH24:MI:SS'') log_date,
                               FILE_NAME,
                               error ERROR_MESSAGE,
                               (SELECT symbol
                                  FROM lg_sal_orders
                                 WHERE id = inlo.object_id)
                                   erp_order_symbol
                          FROM jg_input_log inlo
                         WHERE id ='
                    || r_inlo.id;

                v_xml_clob :=
                    create_xml (v_sql_query,
                                r_inlo.object_type || '_RESPONSE');


                IF v_xml_clob IS NOT NULL
                THEN
                    BEGIN
                        jg_output_sync.send_text_file_to_ftp (
                            p_xml         => v_xml_clob,
                            p_file_name   =>    '/IN/responses/orders/order_'
                                             || r_inlo.file_name);



                        v_xml_response :=
                            NVL (CONCAT (v_xml_response, v_xml_clob),
                                 v_xml_clob);
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            NULL;
                    END;
                END IF;

                UPDATE jg_input_log
                   SET xml_response = v_xml_response
                 WHERE id = r_inlo.id;
            --
            ELSIF    r_inlo.object_type = 'NEW_CONTRACTORS'
                  OR r_inlo.object_type = 'CUSTOMER_DATA'
            THEN
                v_oryginal_id := NULL;

                IF r_inlo.object_type = 'NEW_CONTRACTORS'
                THEN
                    v_sciezka := '/NewCustomer/BasicData/MobizID';
                ELSE
                    v_sciezka := '/CustomerData/BasicData/MobizID';
                END IF;


                BEGIN
                    v_oryginal_id :=
                        pa_xmltype.wartosc (px_xml      => xmltype (r_inlo.xml),
                                            p_sciezka   => v_sciezka);
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        v_oryginal_id := 'TO_CHAR(NULL)';
                END;

                v_sql_query :=
                       'SELECT '
                    || v_oryginal_id
                    || ' MOBIZID,
                               status,
                               TO_CHAR(processed_date,''YYYY-MM-DD HH24:MI:SS'') processed_date,
                               TO_CHAR(log_date,''YYYY-MM-DD HH24:MI:SS'') log_date,
                               FILE_NAME,
                               error ERROR_MESSAGE,
                               (SELECT symbol
                                  FROM ap_kontrahenci
                                 WHERE id = inlo.object_id)
                                   erp_contractor_symbol
                          FROM jg_input_log inlo
                         WHERE id ='
                    || r_inlo.id;

                v_xml_clob :=
                    create_xml (v_sql_query,
                                r_inlo.object_type || '_RESPONSE');

                IF v_xml_clob IS NOT NULL
                THEN
                    BEGIN
                        jg_output_sync.send_text_file_to_ftp (
                            p_xml         => v_xml_clob,
                            p_file_name   =>    '/IN/responses/contractors/contractor_'
                                             || r_inlo.file_name);

                        UPDATE jg_input_log
                           SET xml_response = v_xml_clob
                         WHERE id = r_inlo.id;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            NULL;
                    END;
                END IF;
            ELSIF r_inlo.object_type = 'CASH_RECEIPTS'
            THEN
                v_oryginal_id := NULL;
                v_sciezka := '/NewKPConfirmation/KPNumber';

                BEGIN
                    v_oryginal_id :=
                        pa_xmltype.wartosc (px_xml      => xmltype (r_inlo.xml),
                                            p_sciezka   => v_sciezka);
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        v_oryginal_id := 'TO_CHAR(NULL)';
                END;


                v_sql_query :=
                       'SELECT '''
                    || v_oryginal_id
                    || ''' KPNumber,
                               STATUS,
                               TO_CHAR(processed_date,''YYYY-MM-DD HH24:MI:SS'') processed_date,
                               TO_CHAR(log_date,''YYYY-MM-DD HH24:MI:SS'') log_date,
                               FILE_NAME,
                               ERROR ERROR_MESSAGE
                          FROM jg_input_log inlo
                         WHERE id ='
                    || r_inlo.id;
                set_log (v_sql_query);
                v_xml_clob :=
                    create_xml (v_sql_query,
                                r_inlo.object_type || '_RESPONSE');

                IF v_xml_clob IS NOT NULL
                THEN
                    BEGIN
                        jg_output_sync.send_text_file_to_ftp (
                            p_xml         => v_xml_clob,
                            p_file_name   =>    '/IN/responses/new_kp/new_kp_'
                                             || r_inlo.file_name);

                        UPDATE jg_input_log
                           SET xml_response = v_xml_clob
                         WHERE id = r_inlo.id;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            NULL;
                    END;
                END IF;
            END IF;
        END LOOP;
    END;

    ------------------------------------------------------------------------------------------------------------------------

    PROCEDURE process (pr_operation IN jg_input_log%ROWTYPE)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_object_id   jg_input_log.object_id%TYPE;
    BEGIN
        CASE pr_operation.object_type
            WHEN 'NEW_CONTRACTORS'
            THEN
                v_object_id :=
                    import_customer (
                        p_xml           => pr_operation.xml,
                        p_object_type   => pr_operation.object_type);
            WHEN 'CUSTOMER_DATA'
            THEN
                v_object_id :=
                    import_customer (
                        p_xml           => pr_operation.xml,
                        p_object_type   => pr_operation.object_type);
            WHEN 'ORDER'
            THEN
                v_object_id :=
                    import_sale_order (
                        p_operation_id   => pr_operation.id,
                        p_object_type    => pr_operation.object_type);
            WHEN 'CASH_RECEIPTS'
            THEN
                v_object_id :=
                    import_cash_receipts (
                        p_operation_id   => pr_operation.id,
                        p_object_type    => pr_operation.object_type);
        END CASE;

        save_result (p_inlo_id     => pr_operation.id,
                     p_status      => 'PROCESSED',
                     p_object_id   => v_object_id);
    END;

    ------------------------------------------------------------------------------------------------------------------------

    PROCEDURE get_from_ftp
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_connection    UTL_TCP.connection;
        v_file_list     jg_ftp.t_string_table;
        v_file          CLOB;
        v_object_type   jg_input_log.object_type%TYPE;
        v_on_time       jg_input_log.on_time%TYPE;
        v_error         jg_input_log.error%TYPE;
    BEGIN
        BEGIN
            v_connection :=
                jg_ftp.login (
                    p_host   => jg_ftp_configuration.sf_ftp_host,
                    p_port   => jg_ftp_configuration.sf_ftp_port,
                    p_user   => jg_ftp_configuration.sf_ftp_user,
                    p_pass   => jg_ftp_configuration.sf_ftp_password);

            FOR r_sqre IN (SELECT *
                           FROM jg_sql_repository sqre
                           WHERE sqre.direction = 'IN')
            LOOP
                v_file_list := NULL;
                jg_ftp.nlst (p_conn   => v_connection,
                             p_dir    => r_sqre.file_location,
                             p_list   => v_file_list);

                IF v_file_list.FIRST IS NOT NULL
                THEN
                    FOR v_i IN v_file_list.FIRST .. v_file_list.LAST
                    LOOP
                        BEGIN
                            SAVEPOINT process_file;

                            v_object_type := r_sqre.object_type;

                            IF     v_object_type = 'NEW_CONTRACTORS'
                               AND INSTR (UPPER (v_file_list (v_i)),
                                          'CUSTOMER_DATA') > 0
                            THEN
                                v_object_type := 'CUSTOMER_DATA';
                            END IF;

                            IF INSTR (v_file_list (v_i), '.xml') > 0
                            THEN
                                v_file :=
                                    jg_ftp.get_remote_ascii_data (
                                        p_conn   => v_connection,
                                        p_file   =>    r_sqre.file_location
                                                    || '/'
                                                    || v_file_list (v_i));

                                INSERT INTO jg_input_log (id,
                                                          file_name,
                                                          object_type,
                                                          xml,
                                                          on_time)
                                VALUES (jg_inlo_seq.NEXTVAL,
                                        v_file_list (v_i),
                                        v_object_type,
                                        v_file,
                                        'T');

                                jg_ftp.rename (
                                    p_conn   => v_connection,
                                    p_from   =>    r_sqre.file_location
                                                || '/'
                                                || v_file_list (v_i),
                                    p_to     =>    r_sqre.file_location
                                                || '/archive/'
                                                || v_file_list (v_i));
                            END IF;
                        EXCEPTION
                            WHEN OTHERS
                            THEN
                                ROLLBACK TO process_file;

                                v_error :=
                                       SQLERRM
                                    || CHR (13)
                                    || DBMS_UTILITY.format_error_backtrace;

                                INSERT INTO jg_input_log (id,
                                                          file_name,
                                                          object_type,
                                                          xml,
                                                          on_time,
                                                          status,
                                                          error)
                                VALUES (jg_inlo_seq.NEXTVAL,
                                        v_file_list (v_i),
                                        v_object_type,
                                        v_file,
                                        v_on_time,
                                        'ERROR',
                                        v_error);
                        END;
                    END LOOP;
                END IF;
            END LOOP;

            jg_ftp.LOGOUT (v_connection);
        EXCEPTION
            WHEN OTHERS
            THEN
                jg_ftp.LOGOUT (v_connection);
                assert (
                    FALSE,
                    SQLERRM || '  ' || DBMS_UTILITY.format_error_backtrace);
        END;

        COMMIT;

        FOR r_operation IN (SELECT *
                            FROM jg_input_log
                            WHERE status = 'READY' AND on_time = 'T')
        LOOP
            SAVEPOINT operation;

            BEGIN
                process (pr_operation => r_operation);
                COMMIT;
            EXCEPTION
                WHEN OTHERS
                THEN
                    ROLLBACK TO operation;
                    save_result (
                        p_inlo_id     => r_operation.id,
                        p_status      => 'ERROR',
                        p_object_id   => NULL,
                        p_error       =>    SQLERRM
                                         || CHR (13)
                                         || DBMS_UTILITY.format_error_backtrace);
            END;
        END LOOP;

        send_response;
    END;

    ------------------------------------------------------------------------------------------------------------------------

    PROCEDURE process_all
    IS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        FOR r_operation IN (SELECT *
                            FROM jg_input_log
                            WHERE status = 'READY')
        LOOP
            SAVEPOINT operation;

            BEGIN
                process (pr_operation => r_operation);
            EXCEPTION
                WHEN OTHERS
                THEN
                    ROLLBACK TO operation;

                    save_result (
                        p_inlo_id     => r_operation.id,
                        p_status      => 'ERROR',
                        p_object_id   => NULL,
                        p_error       =>    SQLERRM
                                         || CHR (13)
                                         || DBMS_UTILITY.format_error_backtrace);
            END;
        END LOOP;

        send_response;
    END;
------------------------------------------------------------------------------------------------------------------------

END;
/

CREATE OR REPLACE PACKAGE jg_obop_def
IS
    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE add_operation (
        p_object_id        IN jg_observed_operations.object_id%TYPE,
        p_object_type      IN jg_observed_operations.object_type%TYPE,
        p_operation_type   IN jg_observed_operations.operation_type%TYPE,
        p_attachment       IN jg_observed_operations.attachment%TYPE DEFAULT 'N');
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE PACKAGE BODY jg_obop_def
IS
    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION rt (p_object_id     IN jg_observed_operations.object_id%TYPE,
                 p_object_type   IN jg_observed_operations.object_type%TYPE)
        RETURN jg_observed_operations%ROWTYPE
    IS
        ------------------------------------------------------------------------------------------------------------------------
        CURSOR c_operation (
            pc_object_id      jg_observed_operations.object_id%TYPE,
            pc_object_type    jg_observed_operations.object_type%TYPE)
        IS
            SELECT obop.*
              FROM jg_observed_operations obop
             WHERE     obop.object_id = pc_object_id
                   AND obop.object_type = pc_object_type;

        r_obop   jg_observed_operations%ROWTYPE;
    BEGIN
        OPEN c_operation (p_object_id, p_object_type);

        FETCH c_operation   INTO r_obop;

        CLOSE c_operation;

        RETURN r_obop;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION exist_operation (
        p_object_id     IN jg_observed_operations.object_id%TYPE,
        p_object_type   IN jg_observed_operations.object_type%TYPE)
        RETURN BOOLEAN
    IS
        ------------------------------------------------------------------------------------------------------------------------
        CURSOR c_operation (
            pc_object_id      jg_observed_operations.object_id%TYPE,
            pc_object_type    jg_observed_operations.object_type%TYPE)
        IS
            SELECT obop.id
              FROM jg_observed_operations obop
             WHERE     obop.object_id = pc_object_id
                   AND obop.object_type = pc_object_type;

        v_obop_id   jg_observed_operations.id%TYPE;
    BEGIN
        OPEN c_operation (p_object_id, p_object_type);

        FETCH c_operation   INTO v_obop_id;

        IF c_operation%FOUND
        THEN
            CLOSE c_operation;

            RETURN TRUE;
        END IF;

        CLOSE c_operation;

        RETURN FALSE;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE add_operation (
        p_object_id        IN jg_observed_operations.object_id%TYPE,
        p_object_type      IN jg_observed_operations.object_type%TYPE,
        p_operation_type   IN jg_observed_operations.operation_type%TYPE,
        p_attachment       IN jg_observed_operations.attachment%TYPE DEFAULT 'N')
    IS
        ------------------------------------------------------------------------------------------------------------------------
        r_obop   jg_observed_operations%ROWTYPE;
    BEGIN
        IF p_object_id IS NOT NULL
        THEN
            r_obop := rt (p_object_id, p_object_type);

            IF r_obop.id IS NULL
            THEN
                INSERT INTO jg_observed_operations (id,
                                                    object_type,
                                                    object_id,
                                                    operation_type,
                                                    attachment)
                VALUES (jg_obop_seq.NEXTVAL,
                        p_object_type,
                        p_object_id,
                        p_operation_type,
                        p_attachment);
            ELSE
                IF p_operation_type = 'DELETE'
                THEN
                    DELETE FROM jg_observed_operations
                     WHERE id = r_obop.id;
                END IF;
            END IF;
        END IF;
    END;
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE PACKAGE jg_output_sync
IS
    ------------------------------------------------------------------------------------------------------------------------
    TYPE tt_set_row IS RECORD
    (
        id   NUMBER (10, 0)
    );

    TYPE tt_set_table IS TABLE OF tt_set_row;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE process;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_text_file_to_ftp (p_xml IN CLOB, p_file_name IN VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_binary_file_to_ftp (p_byte        IN BLOB,
                                       p_file_name   IN VARCHAR2);

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION format_number (p_number NUMBER, p_digit INT)
        RETURN VARCHAR2;
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE PACKAGE BODY jg_output_sync
IS
    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION format_number (p_number IN NUMBER, p_digit IN INT)
        RETURN VARCHAR2
    IS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        IF p_number IS NULL
        THEN
            RETURN NULL;
        ELSIF p_number = 0
        THEN
            RETURN 0;
        END IF;

        RETURN TRIM (
                   TRAILING '.' FROM (TRIM (
                                          TRAILING 0 FROM TRIM (
                                                              TO_CHAR (
                                                                  ROUND (
                                                                      p_number,
                                                                      p_digit),
                                                                  '9999999999999999999999999999990.000000000000000000')))));
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION sqre_rt (p_object_type IN jg_sql_repository.object_type%TYPE)
        RETURN jg_sql_repository%ROWTYPE
    IS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        FOR r_sqre
            IN (SELECT *
                FROM jg_sql_repository sqre
                WHERE sqre.object_type = p_object_type AND direction = 'OUT')
        LOOP
            RETURN r_sqre;
        END LOOP;

        RETURN NULL;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE set_batch (
        p_batch_guid    IN jg_observed_operations.batch_guid%TYPE,
        p_object_type   IN jg_sql_repository.object_type%TYPE)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE jg_observed_operations
           SET batch_guid = p_batch_guid
         WHERE     object_type = p_object_type
               AND batch_guid IS NULL
               AND ROWNUM < 500;

        COMMIT;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_query_from_sql_repository (
        p_object_type   IN     jg_sql_repository.object_type%TYPE,
        po_xslt            OUT jg_sql_repository.xslt%TYPE,
        po_batch_guid      OUT jg_observed_operations.batch_guid%TYPE)
        RETURN jg_sql_repository.sql_query%TYPE
    IS
        ------------------------------------------------------------------------------------------------------------------------
        r_sqre   jg_sql_repository%ROWTYPE;
    BEGIN
        r_sqre := sqre_rt (p_object_type);

        IF r_sqre.sql_query IS NULL
        THEN
            assert (
                FALSE,
                   'Brak zdefiniowanego zapytania dla obiektu o typie '''
                || p_object_type
                || '');
        ELSE
            po_xslt := r_sqre.xslt;

            po_batch_guid := SYS_GUID ();
            set_batch (po_batch_guid, p_object_type);

            r_sqre.sql_query :=
                REPLACE (
                    r_sqre.sql_query,
                    ':p_id',
                       'SELECT object_id FROM jg_observed_operations WHERE batch_guid = '''
                    || po_batch_guid
                    || '''');
        END IF;


        RETURN r_sqre.sql_query;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_result (
        p_object_type   IN jg_output_log.object_type%TYPE,
        p_batch_guid    IN jg_observed_operations.batch_guid%TYPE,
        p_xml           IN jg_output_log.xml%TYPE,
        p_status        IN jg_output_log.status%TYPE,
        p_file_name     IN jg_output_log.file_name%TYPE DEFAULT NULL,
        p_error         IN jg_output_log.error%TYPE DEFAULT NULL)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO jg_output_log (id,
                                   object_type,
                                   status,
                                   xml,
                                   error,
                                   file_name,
                                   guid)
        VALUES (jg_oulo_seq.NEXTVAL,
                p_object_type,
                p_status,
                p_xml,
                p_error,
                p_file_name,
                p_batch_guid);

        COMMIT;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_result (
        p_guid        IN jg_output_log.guid%TYPE,
        p_status      IN jg_output_log.status%TYPE,
        p_file_name   IN jg_output_log.file_name%TYPE DEFAULT NULL,
        p_error       IN jg_output_log.error%TYPE DEFAULT NULL)
    IS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        UPDATE jg_output_log oulo
           SET oulo.status = p_status,
               oulo.file_name = p_file_name,
               oulo.error = p_error
         WHERE oulo.guid = p_guid;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_text_file_to_local_folder (p_xml         IN CLOB,
                                              p_file_name   IN VARCHAR2)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        file       UTL_FILE.file_type;
        l_pos      INTEGER := 1;
        xml_len    INTEGER;
        l_amount   BINARY_INTEGER := 32767;
        l_buffer   VARCHAR2 (32767);
    BEGIN
        file :=
            UTL_FILE.fopen (location    => 'INFINITE',
                            filename    => p_file_name,
                            open_mode   => 'w');
        xml_len := DBMS_LOB.getlength (p_xml);

        WHILE l_pos <= xml_len
        LOOP
            DBMS_LOB.read (p_xml,
                           l_amount,
                           l_pos,
                           l_buffer);
            l_buffer := REPLACE (l_buffer, CHR (13), NULL);
            UTL_FILE.put (file => file, buffer => l_buffer);
            l_pos := l_pos + l_amount;
        END LOOP;

        UTL_FILE.fclose (file => file);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_binary_file_to_loc_folder (p_byte        IN BLOB,
                                              p_file_name   IN VARCHAR2)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        file       UTL_FILE.file_type;
        l_pos      INTEGER := 1;
        data_len   INTEGER;
        l_amount   BINARY_INTEGER := 32767;
        l_buffer   RAW (32767);
    BEGIN
        file :=
            UTL_FILE.fopen (location    => 'INFINITE',
                            filename    => p_file_name,
                            open_mode   => 'wb');
        data_len := DBMS_LOB.getlength (p_byte);

        WHILE l_pos <= data_len
        LOOP
            DBMS_LOB.read (p_byte,
                           l_amount,
                           l_pos,
                           l_buffer);
            UTL_FILE.put_raw (file => file, buffer => l_buffer);
            UTL_FILE.fflush (file);

            l_pos := l_pos + l_amount;
        END LOOP;

        UTL_FILE.fclose (file => file);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE generate_attachments (
        p_object_type   IN     jg_observed_operations.object_type%TYPE,
        p_object_id     IN     jg_observed_operations.object_id%TYPE,
        po_file_name       OUT jg_output_log.file_name%TYPE)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        CURSOR c_atta (
            pc_atta_id    pa_attachments.id%TYPE)
        IS
            SELECT atta.file_content file_content,
                   atta.filename file_name,
                   atus.guid object_guid
              FROM pa_attachments atta
                   JOIN pa_attachment_uses atus ON atta.id = atus.atta_id
             WHERE atta.id = pc_atta_id;

        r_atta             c_atta%ROWTYPE;
        v_object_id        NUMBER (10, 0);
        r_sqre             jg_sql_repository%ROWTYPE;
        r_konr             ap_kontrahenci%ROWTYPE;
        r_inma             ap_indeksy_materialowe%ROWTYPE;
        v_file_name        jg_output_log.file_name%TYPE;
        v_file_extension   VARCHAR2 (10);
    BEGIN
        IF p_object_type IN ('CONTRACT_ATTACHMENT')
        THEN
            OPEN c_atta (pc_atta_id => p_object_id);

            FETCH c_atta   INTO r_atta;

            CLOSE c_atta;

            v_object_id :=
                lg_konr_sql.id_guid_uk_wr (p_guid => r_atta.object_guid);

            IF v_object_id IS NOT NULL
            THEN
                r_sqre := sqre_rt ('TRADE_CONTRACTS');

                r_konr := lg_konr_sql.rt (p_id => v_object_id);

                IF r_konr.atrybut_t05 LIKE '%UM IND%'
                THEN
                    v_file_name :=
                           r_sqre.file_location
                        || '/'
                        || r_konr.symbol
                        || '_'
                        || r_konr.nr_umowy_ind
                        || '_'
                        || r_konr.data_umowy_ind;
                ELSE
                    v_file_name :=
                           r_sqre.file_location
                        || '/'
                        || r_konr.symbol
                        || '_'
                        || r_konr.nr_umowy_ind
                        || '_'
                        || r_konr.atrybut_t05;
                END IF;

                v_file_extension :=
                    SUBSTR (r_atta.file_name,
                            INSTR (r_atta.file_name, '.', -1));
                po_file_name := v_file_name || v_file_extension;

                send_binary_file_to_ftp (p_byte        => r_atta.file_content,
                                         p_file_name   => po_file_name);
            END IF;
        ELSIF p_object_type IN ('COMMODITY_ATTACHMENT')
        THEN
            OPEN c_atta (pc_atta_id => p_object_id);

            FETCH c_atta   INTO r_atta;

            CLOSE c_atta;

            r_sqre := sqre_rt ('COMMODITIES');
            r_inma :=
                lg_inma_sql.rt (
                    p_id   => lg_inma_sql.id_guid_uk (r_atta.object_guid));

            po_file_name :=
                   r_sqre.file_location
                || '/'
                || r_inma.indeks
                || '_'
                || r_atta.file_name;
            send_binary_file_to_ftp (p_byte        => r_atta.file_content,
                                     p_file_name   => po_file_name);
        END IF;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE delete_observed_operations (
        p_batch_guid   IN jg_observed_operations.batch_guid%TYPE)
    IS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        DELETE FROM jg_observed_operations
         WHERE batch_guid = p_batch_guid;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    FUNCTION create_xml (
        p_sql_query     IN jg_sql_repository.sql_query%TYPE,
        p_xslt          IN jg_sql_repository.xslt%TYPE,
        p_object_type   IN jg_sql_repository.object_type%TYPE)
        RETURN CLOB
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_ctx              DBMS_XMLSAVE.ctxtype;
        v_xml_clob         CLOB;
        v_xml_type         XMLTYPE;
        r_current_format   pa_xmltype.tr_format;
    BEGIN
        r_current_format := pa_xmltype.biezacy_format;
        pa_xmltype.set_short_format_xml ();

        v_ctx := DBMS_XMLGEN.newcontext (querystring => p_sql_query);
        DBMS_XMLGEN.setrowsettag (v_ctx, p_object_type);
        v_xml_type := DBMS_XMLGEN.getxmltype (v_ctx);

        IF p_xslt IS NOT NULL AND v_xml_type IS NOT NULL
        THEN
            v_xml_type := v_xml_type.transform (xmltype (p_xslt));
        END IF;

        pa_xmltype.ustaw_format (r_current_format);
        DBMS_XMLGEN.closecontext (v_ctx);

        IF v_xml_type IS NOT NULL
        THEN
            v_xml_clob := v_xml_type.getclobval ();
        END IF;

        RETURN v_xml_clob;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_text_file_to_ftp (p_xml IN CLOB, p_file_name IN VARCHAR2)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_connection   UTL_TCP.connection;
    BEGIN
        v_connection :=
            jg_ftp.login (p_host   => jg_ftp_configuration.sf_ftp_host,
                          p_port   => jg_ftp_configuration.sf_ftp_port,
                          p_user   => jg_ftp_configuration.sf_ftp_user,
                          p_pass   => jg_ftp_configuration.sf_ftp_password);

        jg_ftp.put_remote_ascii_data (p_conn   => v_connection,
                                      p_file   => p_file_name,
                                      p_data   => p_xml);

        jg_ftp.get_reply (v_connection);
        jg_ftp.LOGOUT (v_connection);
    EXCEPTION
        WHEN OTHERS
        THEN
            UTL_TCP.close_connection (v_connection);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_binary_file_to_ftp (p_byte        IN BLOB,
                                       p_file_name   IN VARCHAR2)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_connection   UTL_TCP.connection;
    BEGIN
        v_connection :=
            jg_ftp.login (p_host   => jg_ftp_configuration.sf_ftp_host,
                          p_port   => jg_ftp_configuration.sf_ftp_port,
                          p_user   => jg_ftp_configuration.sf_ftp_user,
                          p_pass   => jg_ftp_configuration.sf_ftp_password);

        jg_ftp.put_remote_binary_data (p_conn   => v_connection,
                                       p_file   => p_file_name,
                                       p_data   => p_byte);

        jg_ftp.get_reply (v_connection);
        jg_ftp.LOGOUT (v_connection);
    EXCEPTION
        WHEN OTHERS
        THEN
            UTL_TCP.close_connection (v_connection);
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE process
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_sql_query    jg_sql_repository.sql_query%TYPE;
        v_xml          CLOB;
        v_file_name    jg_output_log.file_name%TYPE;
        v_batch_guid   jg_observed_operations.batch_guid%TYPE;
        v_xslt         jg_sql_repository.xslt%TYPE;
        r_sqre         jg_sql_repository%ROWTYPE;
        v_status       VARCHAR2 (10);
        c_oper         SYS_REFCURSOR;
        v_count        NUMBER;
    BEGIN
        LOOP
            OPEN c_oper FOR
                SELECT COUNT (id)
                  FROM jg_observed_operations
                 WHERE batch_guid IS NULL AND attachment = 'N';

            FETCH c_oper   INTO v_count;

            CLOSE c_oper;

            EXIT WHEN v_count = 0;

            FOR r_operation IN (SELECT object_type
                                FROM jg_observed_operations
                                WHERE attachment = 'N'
                                GROUP BY object_type
                                ORDER BY object_type)
            LOOP
                r_sqre := sqre_rt (r_operation.object_type);
                SAVEPOINT create_xml;

                BEGIN
                    v_sql_query :=
                        get_query_from_sql_repository (
                            r_operation.object_type,
                            v_xslt,
                            v_batch_guid);
                    v_xml :=
                        create_xml (v_sql_query,
                                    v_xslt,
                                    r_operation.object_type);

                    IF v_xml IS NULL
                    THEN
                        v_status := 'NO_DATA';
                    ELSIF r_sqre.up_to_date = 'T'
                    THEN
                        v_status := 'READY';
                    ELSE
                        v_status := 'SKIPPED';
                    END IF;

                    save_result (p_object_type   => r_operation.object_type,
                                 p_batch_guid    => v_batch_guid,
                                 p_xml           => v_xml,
                                 p_status        => v_status);

                    delete_observed_operations (v_batch_guid);
                    COMMIT;
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        ROLLBACK TO create_xml;
                        delete_observed_operations (v_batch_guid);
                        save_result (
                            p_object_type   => r_operation.object_type,
                            p_batch_guid    => v_batch_guid,
                            p_xml           => v_xml,
                            p_status        => 'ERROR',
                            p_error         =>    SQLERRM
                                               || CHR (13)
                                               || DBMS_UTILITY.format_error_backtrace);
                END;
            END LOOP;
        END LOOP;


        FOR r_operation IN (SELECT *
                            FROM jg_output_log
                            WHERE status = 'READY')
        LOOP
            v_status := NULL;

            SAVEPOINT send_file;
            r_sqre := sqre_rt (r_operation.object_type);

            BEGIN
                v_file_name :=
                       NVL (r_sqre.file_location, 'IN/')
                    || '/'
                    || REPLACE (
                              r_operation.object_type
                           || '_'
                           || r_operation.id
                           || '_'
                           || TO_CHAR (SYSTIMESTAMP, 'YYYYMMDD_HH24MISS')
                           || '.xml',
                           '/',
                           '-');


                send_text_file_to_ftp (p_xml         => r_operation.xml,
                                       p_file_name   => v_file_name);

                save_result (p_guid        => r_operation.guid,
                             p_status      => 'PROCESSED',
                             p_file_name   => v_file_name);
                COMMIT;
            EXCEPTION
                WHEN OTHERS
                THEN
                    ROLLBACK TO send_file;
                    save_result (
                        p_guid     => r_operation.guid,
                        p_status   => 'ERROR',
                        p_error    =>    SQLERRM
                                      || CHR (13)
                                      || DBMS_UTILITY.format_error_backtrace);
            END;
        END LOOP;

        FOR r_operation IN (SELECT *
                            FROM jg_observed_operations
                            WHERE attachment = 'T')
        LOOP
            SAVEPOINT send_atta;

            BEGIN
                v_batch_guid := SYS_GUID;
                generate_attachments (
                    p_object_type   => r_operation.object_type,
                    p_object_id     => r_operation.object_id,
                    po_file_name    => v_file_name);

                save_result (p_object_type   => r_operation.object_type,
                             p_batch_guid    => v_batch_guid,
                             p_xml           => NULL,
                             p_file_name     => v_file_name,
                             p_status        => 'PROCESSED');

                DELETE FROM jg_observed_operations
                 WHERE id = r_operation.id;
            EXCEPTION
                WHEN OTHERS
                THEN
                    ROLLBACK TO send_atta;
                    save_result (
                        p_object_type   => r_operation.object_type,
                        p_batch_guid    => v_batch_guid,
                        p_xml           => NULL,
                        p_status        => 'ERROR',
                        p_error         =>    SQLERRM
                                           || CHR (13)
                                           || DBMS_UTILITY.format_error_backtrace);
            END;
        END LOOP;
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE retry (p_id IN jg_output_log.id%TYPE)
    IS
    ------------------------------------------------------------------------------------------------------------------------
    BEGIN
        NULL;
    END;
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE FUNCTION jg_dynamic_set_commponents (
    p_skkp_id    lg_kpl_skladniki_kompletu.id%TYPE)
    RETURN pa_lista_id.tt_lista_id
    PIPELINED
AS
    ------------------------------------------------------------------------------------------------------------------------
    c_inma      SYS_REFCURSOR;
    v_inma_id   ap_indeksy_materialowe.id%TYPE;
    v_sql       lg_kpl_skladniki_kompletu.sql_stmt%TYPE;
BEGIN
    FOR r_skkp IN (SELECT sql_stmt
                   FROM lg_kpl_skladniki_kompletu skkp
                   WHERE skkp.id = p_skkp_id)
    LOOP
        v_sql := r_skkp.sql_stmt;
    END LOOP;

    IF v_sql IS NOT NULL
    THEN
        OPEN c_inma FOR v_sql;

        LOOP
            FETCH c_inma   INTO v_inma_id;

            EXIT WHEN c_inma%NOTFOUND;
            PIPE ROW (v_inma_id);
        END LOOP;

        CLOSE c_inma;
    END IF;

    RETURN;
EXCEPTION
    WHEN OTHERS
    THEN
        CLOSE c_inma;

        RETURN;
END;
/


-----------------------------------------


CREATE OR REPLACE TRIGGER jg_adge_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF nr_domu,
                     miejscowosc,
                     ulica,
                     nr_lokalu
    ON pa_adr_adresy_geograficzne
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    IF pa_wass_def.wartosc (p_nazwa => 'IMPORT_INFINITE') = 'T'
    THEN
        RETURN;
    END IF;

    FOR r_adre IN (SELECT konr_id
                   FROM pa_adr_adresy_kontrahentow_vw
                   WHERE adge_id = :new.id)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_adre.konr_id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'UPDATE');
    END LOOP;

    COMMIT;
END;
/

CREATE OR REPLACE TRIGGER jg_attachments_observe
    BEFORE INSERT OR UPDATE
    ON pa_attachments
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF pa_atki_agd.code (p_id => :new.atki_id) IN ('KONTRAKT')
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACT_ATTACHMENT',
                                   p_operation_type   => 'UPDATE',
                                   p_attachment       => 'T');
    END IF;

    IF pa_atki_agd.code (p_id => :new.atki_id) IN ('INMTR_DKMT')
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'COMMODITY_ATTACHMENT',
                                   p_operation_type   => 'UPDATE',
                                   p_attachment       => 'T');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_cezb_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF konr_id,
                     rcez_id,
                     cena,
                     jdmr_nazwa,
                     grod_id,
                     gras_id,
                     typ,
                     inma_id,
                     cena_brutto
    ON ap_ceny_zbytu
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF :new.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    ELSIF :old.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_deliveries_observe
    BEFORE INSERT OR UPDATE OF wskaznik_zatwierdzenia
    ON ap_dokumenty_obrot
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF     NVL (:old.wskaznik_zatwierdzenia, 'N') = 'N'
       AND :new.wskaznik_zatwierdzenia = 'T'
       AND :new.wzty_kod IN ('WZ')
       AND :new.numer_zamowienia IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'DELIVERIES',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_discounts_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF data_od,
                     data_do,
                     inma_id,
                     grod_id,
                     upust_procentowy,
                     konr_id,
                     gras_id
    ON lg_przyp_upustow
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'DISCOUNTS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'DISCOUNTS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_foza_observe
    BEFORE INSERT OR DELETE OR UPDATE OF opis, typ
    ON ap_formy_zaplaty
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'PAYMENTS_METHODS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'PAYMENTS_METHODS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_grin_observe
    BEFORE INSERT OR DELETE OR UPDATE OF podstawowa, inma_id
    ON ap_grupy_indeksow
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF :new.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    ELSIF :old.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_inma_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF jdmr_nazwa,
                     stva_id,
                     cecha,
                     nazwa,
                     id,
                     indeks
    ON ap_indeksy_materialowe
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'INSERT');
    ELSIF UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_invoice_eksport_trg
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_sal_invoices
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    --  ASSERT(FALSE,:NEW.DOC_TYPE||'#'|| :NEW.approved);
    IF :new.doc_type IN ('FS',
                         'KS',
                         'FE',
                         'KE')
    THEN
        IF     :new.approved = 'T'
           AND NVL (:old.approved, 'N') = 'N'
           AND (INSERTING OR UPDATING)
        THEN
            jg_obop_def.add_operation (p_object_id        => :new.id,
                                       p_object_type      => 'INVOICES',
                                       p_operation_type   => 'INSERT');
        END IF;
    END IF;

    IF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'INVOICES',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_kngr_observe
    BEFORE INSERT OR DELETE OR UPDATE OF grkn_id, konr_id, id
    ON lg_kontrahenci_grup
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    FOR r_osol
        IN (SELECT osol.id
            FROM lg_osoby_log osol,
                 (SELECT *
                    FROM lg_grupy_kontrahentow
                  START WITH id = 63
                  CONNECT BY PRIOR id = grkn_id) grko
            WHERE     osol.atrybut_t01 = grko.nazwa
                  AND grko.id IN (:new.grkn_id, :old.grkn_id))
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_osol.id,
                                   p_object_type      => 'SALES_REPRESENTATIVES',
                                   p_operation_type   => 'INSERT');
    END LOOP;


    FOR r_osol
        IN (SELECT osol.id
            FROM lg_osoby_log osol,
                 (SELECT *
                    FROM lg_grupy_kontrahentow
                  START WITH id = 63
                  CONNECT BY PRIOR id = grkn_id) grko,
                 lg_kontrahenci_grup kngr
            WHERE     osol.atrybut_t01 = grko.nazwa
                  AND grko.id = kngr.grkn_id
                  AND osol.aktualna = 'T'
                  AND kngr.konr_id IN (:new.konr_id, :old.konr_id))
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_osol.id,
                                   p_object_type      => 'SALES_REPRESENTATIVES',
                                   p_operation_type   => 'INSERT');
    END LOOP;

    COMMIT;
END;
/

CREATE OR REPLACE TRIGGER jg_konr_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF id,
                     aktualny,
                     symbol,
                     skrot,
                     odbiorca,
                     mail,
                     nazwa,
                     nr_umowy_ind,
                     platnik,
                     platnik_id,
                     nr_tel,
                     nip,
                     nr_faksu,
                     blokada_sprz,
                     dni_do_zaplaty
    ON ap_kontrahenci
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF pa_wass_def.wartosc (p_nazwa => 'IMPORT_INFINITE') = 'T'
    THEN
        RETURN;
    END IF;

    IF INSERTING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'INSERT');
    ELSIF UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_likr_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF data_do,
                     wartosc,
                     data_od,
                     konr_id
    ON lg_knr_limity_kredyt
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF pa_wass_def.wartosc (p_nazwa => 'IMPORT_INFINITE') = 'T'
    THEN
        RETURN;
    END IF;

    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.konr_id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.konr_id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_loyality_points_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_plo_punkty_kontrahenta
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'LOYALITY_POINTS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'LOYALITY_POINTS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_order_patterns
    BEFORE INSERT OR UPDATE
    ON lg_documents_templates
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF :new.document_type IN ('ZS', 'ZE')
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'ORDERS_PATTERNS',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_osol_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF aktualna,
                     first_name,
                     id,
                     surname,
                     kod,
                     atrybut_t01
    ON lg_osoby_log
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'SALES_REPRESENTATIVES',
                                   p_operation_type   => 'INSERT');
    ELSIF UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'SALES_REPRESENTATIVES',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'SALES_REPRESENTATIVES',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_prje_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF id,
                     inma_id,
                     jdmr_nazwa,
                     kod_kreskowy
    ON lg_przeliczniki_jednostek
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    jg_obop_def.add_operation (p_object_id        => :new.inma_id,
                               p_object_type      => 'COMMODITIES',
                               p_operation_type   => 'UPDATE');
END;
/

CREATE OR REPLACE TRIGGER jg_reze_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_rzm_rezerwacje
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF lg_rzm_zare_agd.zrre_typ (p_id => NVL (:new.zare_id, :old.zare_id)) =
           'ZASI'
    THEN
        IF INSERTING OR UPDATING
        THEN
            jg_obop_def.add_operation (p_object_id        => :new.id,
                                       p_object_type      => 'RESERVATIONS',
                                       p_operation_type   => 'UPDATE');
        ELSIF DELETING
        THEN
            jg_obop_def.add_operation (p_object_id        => :old.id,
                                       p_object_type      => 'RESERVATIONS',
                                       p_operation_type   => 'DELETE');
        END IF;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_rond_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON rk_rozr_nal_dokumenty
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF     (INSERTING OR UPDATING)
       AND :new.pozostalo_do_zaplaty_z_kor != :old.pozostalo_do_zaplaty_z_kor
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'INVOICES_PAYMENTS',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_sets_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_kpl_skladniki_kompletu
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.kpl_inma_id,
                                   p_object_type      => 'SETS_COMPONENTS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.kpl_inma_id,
                                   p_object_type      => 'SETS_COMPONENTS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_sinv_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_sal_invoices
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF     (INSERTING OR UPDATING)
       AND :new.approved = 'T'
       AND NVL (:old.approved, 'N') = 'N'
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'INVOICES',
                                   p_operation_type   => 'INSERT');
    ELSIF    (DELETING)
          OR     (UPDATING)
             AND :old.approved = 'T'
             AND NVL (:new.approved, 'N') = 'N'
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'INVOICES',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_spdo_observe
    BEFORE INSERT OR DELETE OR UPDATE OF opis, transport_wlasny
    ON ap_sposoby_dostaw
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'DELIVERY_METHODS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'DELIVERY_METHODS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_support_fund_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_sal_invoices
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF INSERTING OR UPDATING
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'SUPPORT_FUNDS',
                                   p_operation_type   => 'UPDATE');
    ELSIF DELETING
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.id,
                                   p_object_type      => 'SUPPORT_FUNDS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_trade_contracts_observe
    BEFORE INSERT OR
           UPDATE OF atrybut_n04,
                     dni_do_zaplaty,
                     atrybut_n05,
                     foza_kod,
                     atrybut_n03,
                     atrybut_n02,
                     atrybut_t07,
                     limit_kredytowy
    ON ap_kontrahenci
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF :new.atrybut_t05 IS NULL
    THEN
        RETURN;
    END IF;

    IF INSERTING OR UPDATING
    THEN
        IF :new.atrybut_t05 LIKE '%UM IND%'
        THEN
            jg_obop_def.add_operation (
                p_object_id        => :new.id,
                p_object_type      => 'TRADE_CONTRACTS_INDIVIDUAL',
                p_operation_type   => 'UPDATE');
        ELSE
            jg_obop_def.add_operation (p_object_id        => :new.id,
                                       p_object_type      => 'TRADE_CONTRACTS',
                                       p_operation_type   => 'UPDATE');
        END IF;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_umsp_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_ums_umowy_sprz
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF     (INSERTING OR UPDATING)
       AND :new.zatwierdzona = 'T'
       AND NVL (:old.zatwierdzona, 'N') = 'N'
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACTS',
                                   p_operation_type   => 'INSERT');
    ELSIF    (DELETING)
          OR     (UPDATING)
             AND :old.zatwierdzona = 'T'
             AND NVL (:new.zatwierdzona, 'N') = 'N'
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.id,
                                   p_object_type      => 'CONTRACTS',
                                   p_operation_type   => 'DELETE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_uzad_observe
    BEFORE INSERT OR DELETE OR UPDATE
    ON lg_pdm_uzycia_adresow
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    IF pa_wass_def.wartosc (p_nazwa => 'IMPORT_INFINITE') = 'T'
    THEN
        RETURN;
    END IF;

    FOR r_adre IN (SELECT konr_id
                   FROM pa_adr_adresy_kontrahentow_vw
                   WHERE uzad_id = :new.id)
    LOOP
        jg_obop_def.add_operation (p_object_id        => r_adre.konr_id,
                                   p_object_type      => 'CONTRACTORS',
                                   p_operation_type   => 'UPDATE');
    END LOOP;

    COMMIT;
END;
/

CREATE OR REPLACE TRIGGER jg_wace_observe
    BEFORE INSERT OR
           DELETE OR
           UPDATE OF data_od,
                     data_do,
                     price_min_net,
                     jdmr_nazwa,
                     price_min_gross,
                     inma_id
    ON lg_wah_warunki_cen
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    IF :new.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :new.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    ELSIF :old.inma_id IS NOT NULL
    THEN
        jg_obop_def.add_operation (p_object_id        => :old.inma_id,
                                   p_object_type      => 'COMMODITIES',
                                   p_operation_type   => 'UPDATE');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER jg_warehouse_observe
    BEFORE INSERT OR UPDATE OF stan_goracy
    ON ap_stany_magazynowe
    REFERENCING NEW AS new OLD AS old
    FOR EACH ROW
BEGIN
    jg_obop_def.add_operation (p_object_id        => :new.id,
                               p_object_type      => 'WAREHOUSES',
                               p_operation_type   => 'UPDATE');
END;
/



--SELECT host, lower_port, upper_port, acl
--  FROM dba_network_acls
----------------------------------------------------------------------------


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


------------------------------------------ data


DECLARE
    v_order_clob   CLOB;
    v_xslt         CLOB;
BEGIN
    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   file_location,
                                   up_to_date,
                                   direction)
    VALUES (jg_sqre_seq.NEXTVAL,
            'CASH_RECEIPTS',
            'OUT/new_kp',
            'T',
            'IN');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'INVOICES_PAYMENTS',
                   '
SELECT rndo.symbol_dokumentu invoice_number,
         rndo.data_dokumentu invoice_date,
         rndo.termin_platnosci due_date,
         rndo.forma_platnosci payment_form,
         konr.symbol payer_symbol,
         konr.nazwa payer_name,
         jg_output_sync.format_number (rndo.wartosc_dok_z_kor_wwb, 2) total,
         jg_output_sync.format_number (rndo.poz_do_zaplaty_dok_z_kor_wwb, 2)
             amount_left,
         CURSOR (
             SELECT rnwp.symbol_dokumentu payment_doc_number,
                    rnwp.data_dokumentu payment_date,
                    jg_output_sync.format_number (rnwp.zaplata_wwb, 2)
                        amount_paid
               FROM rk_rozr_nal_dok_plat_rk_vw rnwp
              WHERE     rnwp.rndo_id = rndo.rndo_id
                    AND rnwp.zaplata_wwb IS NOT NULL
                    AND rnwp.typ = ''P'')
             payments_details
    FROM rk_rozr_nal_dokumenty_vw rndo, ap_kontrahenci konr
   WHERE     konr.id = rndo.konr_id
         AND rndo.rnwp_rnwp_id IS NULL
         AND rndo.typ IN (''FAK'', ''KOR'')
         and rndo.poz_do_zaplaty_dok_z_kor_wwb > 0
         AND rndo.rndo_id IN ( :p_id)
GROUP BY rndo.symbol_dokumentu,
         rndo.termin_platnosci,
         rndo.forma_platnosci,
         konr.id,
         konr.symbol,
         konr.nazwa,
         rndo.wartosc_dok_z_kor_wwb,
         rndo.poz_do_zaplaty_dok_z_kor_wwb,
         rndo.rndo_id,
          rndo.data_dokumentu',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <INVOICE_PAYMENTS><xsl:apply-templates/></INVOICE_PAYMENTS>
                     </xsl:template>
                     <xsl:template priority="2" match="PAYMENTS_DETAILS/PAYMENTS_DETAILS_ROW">
                        <PAYMENT_DETAIL><xsl:apply-templates/></PAYMENT_DETAIL>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/invoices_payments',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'COMMODITIES',
                   'SELECT inma.indeks item_index,
       inma.nazwa name,
       ec ec,
       inma.jdmr_nazwa base_unit_of_measure_code,
       BIN_TO_NUM (DECODE (atrybut_c01, ''T'', 1, 0),
                   DECODE (ec, ''T'', 1, 0),
                   DECODE (w_ofercie, ''T'', 1, 0),
                   DECODE (mozliwe_sprzedawanie, ''T'', 1, 0)) AVAILABILITY,
       (SELECT MAX (kod_kreskowy) ean
          FROM lg_przeliczniki_jednostek prje
         WHERE     prje.kod_kreskowy IS NOT NULL
               AND prje.inma_id = inma.id
               AND prje.jdmr_nazwa = inma.jdmr_nazwa)
           base_ean_code,
       (SELECT rv_meaning
          FROM cg_ref_codes
         WHERE rv_domain = ''LG_CECHY_INMA'' AND rv_low_value = inma.cecha)
           TYPE,
       (SELECT stva.stopa
          FROM rk_stawki_vat stva
         WHERE stva.id = inma.stva_id)
           vat_rate,
       NVL ( (SELECT zapas_min
                FROM ap_inma_maga_zapasy inmz
               WHERE inmz.inma_id = inma.id AND inmz.maga_id = 500),
            0)
           min_stock,
       CURSOR (SELECT jdmr_nazwa unit_of_measure_code, kod_kreskowy ean_code
                 FROM lg_przeliczniki_jednostek prje
                WHERE prje.inma_id = inma.id)
           units_of_measure,
       CURSOR (
           SELECT walu.kod currency,
                  jg_output_sync.format_number (cezb.cena, 4) net_price,
                  jg_output_sync.format_number (cezb.cena_brutto, 4)
                      gross_price,
                  cezb.jdmr_nazwa unit_of_measure_code,
                  rcez.rodzaj price_type
             FROM ap_ceny_zbytu cezb,
                  ap_rodzaje_ceny_zbytu rcez,
                  rk_waluty walu
            WHERE     cezb.rcez_id = rcez.id
                  AND cezb.typ = ''SPRZEDAZ''
                  AND cezb.grod_id IS NULL
                  AND cezb.gras_id IS NULL
                  AND cezb.konr_id IS NULL
                  AND walu.id = cezb.walu_id
                  AND cezb.sprzedaz = ''T''
                  AND lg_cezb_sql.aktualna_tn (cezb.id) = ''T''
                  AND cezb.inma_id = inma.id)
           prices,
       CURSOR (
           SELECT jg_output_sync.format_number (wace.price_min_net, 4)
                      net_price,
                  jg_output_sync.format_number (wace.price_min_gross, 4)
                      gross_price,
                  wace.jdmr_nazwa unit_of_measure_code
             FROM lg_wah_warunki_cen wace
            WHERE     wace.price_min_net IS NOT NULL
                  AND wace.price_min_gross IS NOT NULL
                  AND wace.data_od <= SYSDATE
                  AND (wace.data_do >= SYSDATE OR wace.data_do IS NULL)
                  AND wace.inma_id = inma.id)
           minimal_prices,
       CURSOR (
           SELECT gras.grupa_asortymentowa group_name,
                  gras.kod group_code,
                  grin.podstawowa is_primary
             FROM ap_grupy_indeksow grin, ap_grupy_asortymentowe gras
            WHERE     gras.id = grin.gras_id
                  AND grin.inma_id = inma.id
                  AND gras.id IN (SELECT gras.id
                                    FROM ap_grupy_asortymentowe gras
                                  CONNECT BY PRIOR gras.id = gras.gras_id_nad
                                  START WITH gras.kod = ''GRAS 2013''))
           groups
  FROM ap_indeksy_materialowe inma
 WHERE inma.aktualny = ''T'' AND inma.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="UNITS_OF_MEASURE/UNITS_OF_MEASURE_ROW">
                        <UNIT_OF_MEASURE><xsl:apply-templates/></UNIT_OF_MEASURE>
                     </xsl:template>
                     <xsl:template priority="2" match="ROW">
                        <COMMODITY><xsl:apply-templates/></COMMODITY>
                     </xsl:template>
                     <xsl:template priority="2" match="PRICES/PRICES_ROW">
                        <PRICE><xsl:apply-templates/></PRICE>
                     </xsl:template>
                     <xsl:template priority="2" match="MINIMAL_PRICES/MINIMAL_PRICES_ROW">
                        <MINIMAL_PRICE><xsl:apply-templates/></MINIMAL_PRICE>
                     </xsl:template>
                     <xsl:template priority="2" match="GROUPS/GROUPS_ROW">
                        <GROUP><xsl:apply-templates/></GROUP>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/commodities',
                   'T',
                   'OUT');


    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'CONTRACTORS',
                   'SELECT konr.symbol customer_number,
                         konr_payer.symbol payer_number,
                         konr.nazwa name,
                         konr.skrot short_name,
                         konr.nip nip,
                         konr.blokada_sprz order_blockade,
                         NVL(konr.aktualny, ''N'') active,
                         konr.platnik is_payer,
                         konr.odbiorca is_reciever,
                         konr.potential potential,
                         konr.nr_tel phone,
                         konr.nr_faksu fax,
                         konr.mail email,
                         --konr.dni_do_zaplaty day_topay,
                         jg_output_sync.format_number(lg_knr_likr_sql.aktualny_limit_konr_kwota(konr.id, pa_sesja.dzisiaj), 2) credit_limit,
                         (SELECT grupa
                            FROM ap_grupy_odbiorcow grod
                           WHERE grod.id = konr.grod_id) reciever_group,
                         (SELECT MAX(osol.kod)
                            FROM lg_osoby_log osol,
                         (SELECT *
                            FROM lg_grupy_kontrahentow
                           START WITH id = 63
                      CONNECT BY PRIOR id = grkn_id) grko,
                                 lg_kontrahenci_grup kngr
                           WHERE     osol.atrybut_t01 = grko.nazwa
                                 AND grko.id = kngr.grkn_id
                                 AND osol.aktualna = ''T''
                                 AND kngr.konr_id = konr.id) representative,
                                     konr.foza_kod default_financing_method,
                          CURSOR (SELECT ulica        street,
                                         nr_domu      house_number,
                                         nr_lokalu    flat_number,
                                         miejscowosc  city,
                                         kod_pocztowy post_code
                                    FROM lg_kntrh_adresy_konr_vw
                                   WHERE     konr_id = konr.id
                                         AND typ_adresu = ''GEOGRAFICZNY''
                                         AND rola_adresu = ''SIEDZIBY'') legal_addresses,
                          CURSOR (SELECT ulica        street,
                                         nr_domu      house_number,
                                         nr_lokalu    flat_number,
                                         miejscowosc  city,
                                         kod_pocztowy post_code
                                    FROM lg_kntrh_adresy_konr_vw
                                   WHERE     konr_id = konr.id
                                         AND typ_adresu = ''GEOGRAFICZNY''
                                         AND rola_adresu = ''DOSTAWY'') delivery_addresses
                    FROM ap_kontrahenci konr, ap_kontrahenci konr_payer
                   WHERE     konr_payer.id(+) = konr.platnik_id
                         AND konr.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                    <xsl:output method="xml" version="1.0" indent="yes" omit-xml-declaration="no" />
                    <xsl:strip-space elements="*"/>
                    <xsl:template match="node()|@*">
                       <xsl:copy>
                          <xsl:apply-templates select="node()|@*"/>
                       </xsl:copy>
                    </xsl:template>
                    <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                    <xsl:template priority="2" match="ROW">
                       <CONTRACTOR><xsl:apply-templates/></CONTRACTOR>
                    </xsl:template>
                    <xsl:template priority="2" match="LEGAL_ADDRESSES/LEGAL_ADDRESSES_ROW">
                       <LEGAL_ADDRESS><xsl:apply-templates/></LEGAL_ADDRESS>
                    </xsl:template>
                    <xsl:template priority="2" match="DELIVERY_ADDRESSES/DELIVERY_ADDRESSES_ROW">
                       <DELIVERY_ADDRESS><xsl:apply-templates/></DELIVERY_ADDRESS>
                    </xsl:template>
                 </xsl:stylesheet>',
                   'IN/contractors',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'INVOICES',
                   'SELECT header.symbol invoice_symbol,
       (SELECT symbol
          FROM lg_sal_orders sord
         WHERE sord.id = header.source_order_id)
           order_symbol,
       header.doc_type,
       header.doc_date invoice_date,
       header.sale_date sale_date,
       header.payment_date payment_date,
       header.currency currency,
       jg_output_sync.format_number (header.net_value, 2) net_value,
       jg_output_sync.format_number (header.gross_value, 2) gross_value,
       jg_output_sync.format_number (lg_dosp_sql.kwota_zaplat_na_dok (id), 2)
           amount_paid,
       CASE
           WHEN header.gross_value <= lg_dosp_sql.kwota_zaplat_na_dok (id)
           THEN
               ''T''
           ELSE
               ''N''
       END
           is_paid,
       header.payer_symbol payer_symbol,
       header.payer_name,
       header.payer_nip,
       header.payer_city,
       header.payer_postal_code payer_post_code,
       header.payer_street,
       header.payer_building,
       header.payer_apartment,
       header.receiver_symbol,
       header.receiver_name,
       header.delivery_type,
       CURSOR (
           SELECT ordinal ordinal,
                  item_symbol item_symbol,
                  item_name item_name,
                  unit unit_of_measure_code,
                  jg_output_sync.format_number (quantity, 100) quantity,
                  jg_output_sync.format_number (net_price, 2) net_price,
                  jg_output_sync.format_number (vat_percent, 2) vat_rate,
                  jg_output_sync.format_number (net_value, 2) net_value,
                  jg_output_sync.format_number (vat_value, 2) vat_value,
                  jg_output_sync.format_number (gross_value, 2) gross_value
             FROM lg_sal_invoices_it
            WHERE line_type IN (''N'', ''P'') AND document_id = header.id)
           lines
  FROM lg_sal_invoices header
 WHERE     header.approved = ''T''
       AND doc_type IN (''FS'', ''KS'')
       AND header.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <INVOICE><xsl:apply-templates/></INVOICE>
                     </xsl:template>
                     <xsl:template priority="2" match="LINES/LINES_ROW">
                        <LINE><xsl:apply-templates/></LINE>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/invoices',
                   'T',
                   'OUT');

    v_order_clob :=
        'SELECT header.*,
                sord.guid,
                            TRUNC(TO_DATE(header.order_issue_date_bc, ''YYYY-MM-DD"T"HH24:MI:SS'')) order_issue_date,
                            TRUNC(TO_DATE(header.requested_delivery_date_bc, ''YYYY-MM-DD"T"HH24:MI:SS'')) requested_delivery_date,
                            wzrc.document_type document_type,
                            wzrc.pricing_type pricing_type,
                            pa_firm_sql.kod (wzrc.firm_id) company_code,
                            wzrc.place_of_issue place_of_issue,
                            NVL (wzrc.base_currency, wzrc.currency) currency,
                            NVL(header.payment_date, wzrc.payment_days) payment_days,
                            pusp.kod pusp_kod,
                            NVL(header.net_value * (header.order_discount/ 100), 0) order_discount_value,
                            CURSOR ( SELECT konr.symbol,
                                            konr.nazwa,
                                            konr.skrot,
                                            konr.nip,
                                            adge.miejscowosc,
                                            adge.kod_pocztowy,
                                            adge.ulica,
                                            adge.nr_domu,
                                            adge.nr_lokalu,
                                            adge.poczta
                                       FROM ap_kontrahenci konr, pa_adr_adresy_geograficzne adge
                                      WHERE     adge.id = lg_konr_adresy.adge_id_siedziby (konr.id)
                                            AND konr.id = wzrc.issuer_id) sprzedawca,
                            CURSOR ( SELECT konr.symbol,
                                            konr.nazwa,
                                            konr.skrot,
                                            konr.nip,
                                            adge.miejscowosc,
                                            adge.kod_pocztowy,
                                            adge.ulica,
                                            adge.nr_domu,
                                            adge.nr_lokalu,
                                            adge.poczta
                                       FROM ap_kontrahenci konr, pa_adr_adresy_geograficzne adge
                                      WHERE     adge.id = lg_konr_adresy.adge_id_siedziby (konr.id)
                                            AND konr.symbol = header.seller_buyer_id) platnik,
                            CURSOR ( SELECT konr.symbol,
                                            konr.nazwa,
                                            konr.skrot,
                                            konr.nip,
                                            adge.miejscowosc,
                                            adge.kod_pocztowy,
                                            adge.ulica,
                                            adge.nr_domu,
                                            adge.nr_lokalu,
                                            adge.poczta
                                       FROM ap_kontrahenci konr, pa_adr_adresy_geograficzne adge
                                      WHERE     adge.id = lg_konr_adresy.adge_id_siedziby (konr.id)
                                            AND konr.symbol = header.receiver_id ) odbiorca,
                            CURSOR ( SELECT item_xml.*,
                                            sori.guid,
                                            (item_xml.unit_price_base - item_xml.unit_price_value) discount_value,
                                            inma.nazwa commodity_name,
                                            inma.jdmr_nazwa_pdst_sp jdmr_nazwa,
                                            api_rk_stva.kod (inma.stva_id) inma_stva_code,
                                            NVL (wzrc.base_currency, wzrc.currency) currency
                                       FROM jg_input_log log1,
                                            ap_indeksy_materialowe inma,
                                            XMLTABLE ( ''//Order/OrderDetail/Item''
                                                       PASSING xmltype (log1.xml)
                                                       COLUMNS item_num               VARCHAR2 (30) PATH ''/Item/ItemNum'',
                                                               seller_item_id         VARCHAR2 (30) PATH ''/Item/SellerItemID'',
                                                               name                   VARCHAR2 (70) PATH ''/Item/Name'',                                                               
                                                               unit_of_measure        VARCHAR2 (30) PATH ''/Item/UnitOfMeasure'',                                                               
                                                               quantity_value         VARCHAR2 (30) PATH ''/Item/QuantityValue'',
                                                               tax_percent            VARCHAR2 (30) PATH ''/Item/TaxPercent'',
                                                               unit_price_value       VARCHAR2 (30) PATH ''/Item/UnitPriceValue'',
                                                               unit_price_base        VARCHAR2 (30) PATH ''/Item/UnitPriceBase'',
                                                               unit_discount_value    VARCHAR2 (30) PATH ''/Item/UnitDiscountValue'',
                                                               unit_discount          VARCHAR2 (30) PATH ''/Item/UnitDiscount'',
                                                               description                  VARCHAR2(500) PATH ''/Item/Description'',
                                                               promotion_code         VARCHAR2 (500) PATH ''/Item/PromotionCode'',
                                                               promotion_name         VARCHAR2 (500) PATH ''/Item/PromotionName'') item_xml,
                                            lg_sal_orders_it sori                                                           
                                      WHERE     log1.id = LOG.id
                                            AND inma.indeks = item_xml.seller_item_id
                                            AND (    sori.document_id (+) = sord.id
                                                 AND sori.item_symbol (+) = item_xml.seller_item_id)
                                                 AND sori.ordinal (+) = item_xml.item_num) items
                       FROM jg_input_log LOG,
                            lg_documents_templates wzrc,
                            lg_punkty_sprzedazy pusp,
                            XMLTABLE ( ''//Order''
                                       PASSING xmltype (LOG.xml)
                                       COLUMNS order_number               VARCHAR2 (30)      PATH ''/Order/OrderHeader/OrderNumber'',
                                              order_pattern                         VARCHAR2 (100)     PATH ''/Order/OrderHeader/OrderPattern'',
                                              order_type                              VARCHAR2 (1)       PATH ''/Order/OrderHeader/OrderType'',
                                              order_issue_date_bc              VARCHAR2 (30)      PATH ''/Order/OrderHeader/OrderIssueDate'',
                                              requested_delivery_date_bc  VARCHAR2 (30)      PATH ''/Order/OrderHeader/RequestedDeliveryDate'',
                                              note                                         VARCHAR2 (100)     PATH ''/Order/OrderHeader/Comment'',
                                              payment_date                         VARCHAR2(30)     PATH ''/Order/OrderHeader/PaymentDate'',
                                              order_discount                        VARCHAR2 (1)       PATH ''/Order/OrderHeader/OrderDiscount'',                                               
                                              payment_method_code          VARCHAR2 (6)       PATH ''/Order/OrderHeader/PaymentMethod/Code'',
                                              transportation_code               VARCHAR2 (3)       PATH ''/Order/OrderHeader/Transportation/Code'',                                               
                                              seller_buyer_id                       VARCHAR2 (30)      PATH ''/Order/OrderParty/BuyerParty/SellerBuyerID'',
                                              seller_contact_tel                   VARCHAR2 (30)      PATH ''/Order/OrderParty/BuyerParty/Contact/Tel'',
                                              receiver_id                              VARCHAR2(30)       PATH ''/Order/OrderParty/ShipToParty/CustomerNumber'',
                                              sr_party_description              VARCHAR2 (30)      PATH ''/Order/OrderParty/SRParty/Description'',
                                              net_value                               VARCHAR2(30)        PATH ''/Order/OrderSummary/TotalNetAmount'',
                                              gross_value                            VARCHAR2(30)        PATH ''/Order/OrderSummary/TotalGrossAmount'' ) header,
                            lg_sal_orders sord
                      WHERE     pusp.id = wzrc.pusp_id
                            AND wzrc.pattern = header.order_pattern
                            AND (    sord.doc_symbol_rcv(+) = header.order_number
                                 AND sord.payer_symbol(+) = header.seller_buyer_id)
                            AND LOG.id = :p_operation_id';

    v_xslt :=
        '<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" encoding="windows-1250" indent="yes"/>
  <xsl:template match="/">
    <LG_ZASP_T>
      <xsl:for-each select="ORDER">
        <xsl:for-each select="ORDER_NUMBER">
          <SYMBOL_ODBIORCY>
            <xsl:value-of select="."/>
          </SYMBOL_ODBIORCY>
        </xsl:for-each>
        <xsl:for-each select="GUID">
          <GUID_DOKUMENTU>
            <xsl:value-of select="."/>
          </GUID_DOKUMENTU>
        </xsl:for-each>
        <xsl:for-each select="ORDER_PATTERN">
          <WZORZEC>
            <xsl:value-of select="."/>
          </WZORZEC>
        </xsl:for-each>
        <xsl:for-each select="DOCUMENT_TYPE">
          <TYP_ZAMOWIENIA>
            <xsl:value-of select="."/>
          </TYP_ZAMOWIENIA>
        </xsl:for-each>
        <xsl:for-each select="ORDER_ISSUE_DATE">
          <DATA_WYSTAWIENIA>
            <xsl:value-of select="."/>
          </DATA_WYSTAWIENIA>
        </xsl:for-each>
        <xsl:for-each select="REQUESTED_DELIVERY_DATE">
          <DATA_REALIZACJI>
            <xsl:value-of select="."/>
          </DATA_REALIZACJI>
        </xsl:for-each>
        <xsl:for-each select="PLACE_OF_ISSUE">
          <MIEJSCE_WYSTAWIENIA>
            <xsl:value-of select="."/>
          </MIEJSCE_WYSTAWIENIA>
        </xsl:for-each>
        <xsl:for-each select="COMPANY_CODE">
          <KOD_FIRMY>
            <xsl:value-of select="."/>
          </KOD_FIRMY>
        </xsl:for-each>
        <xsl:for-each select="NOTE">
          <UWAGI>
            <xsl:value-of select="."/>
          </UWAGI>
        </xsl:for-each>
        <xsl:for-each select="CURRENCY">
          <KOD_WALUTY_CENNIKA>
            <xsl:value-of select="."/>
          </KOD_WALUTY_CENNIKA>
        </xsl:for-each>
        <xsl:for-each select="PAYMENT_METHOD_CODE">
          <KOD_FORMY_ZAPLATY>
            <xsl:value-of select="."/>
          </KOD_FORMY_ZAPLATY>
        </xsl:for-each>
        <xsl:for-each select="TRANSPORTATION_CODE">
          <KOD_SPOSOBU_DOSTAWY>
            <xsl:value-of select="."/>
          </KOD_SPOSOBU_DOSTAWY>
        </xsl:for-each>
        <xsl:for-each select="PRICING_TYPE">
          <WG_JAKICH_CEN>
            <xsl:value-of select="."/>
          </WG_JAKICH_CEN>
        </xsl:for-each>
        <xsl:for-each select="ORDER_DISCOUNT_VALUE">
          <OPUST_GLOB_KWOTA>
            <xsl:value-of select="."/>
          </OPUST_GLOB_KWOTA>
        </xsl:for-each>
        <xsl:for-each select="ORDER_DISCOUNT">
          <OPUST_GLOB_PROC_OD_WART_BEZ_UP>
            <xsl:value-of select="."/>
          </OPUST_GLOB_PROC_OD_WART_BEZ_UP>
        </xsl:for-each>
        <xsl:for-each select="PAYMENT_DAYS">
          <ILOSC_DNI_DO_ZAPLATY>
            <xsl:value-of select="."/>
          </ILOSC_DNI_DO_ZAPLATY>
        </xsl:for-each>
        <xsl:for-each select="PUSP_KOD">
          <KOD_PUNKTU_SPRZEDAZY>
            <xsl:value-of select="."/>
          </KOD_PUNKTU_SPRZEDAZY>
        </xsl:for-each>
        <xsl:for-each select="NET_VALUE">
          <WARTOSC_NETTO>
            <xsl:value-of select="."/>
          </WARTOSC_NETTO>
        </xsl:for-each>
        <xsl:for-each select="GROSS_VALUE">
          <WARTOSC_BRUTTO>
            <xsl:value-of select="."/>
          </WARTOSC_BRUTTO>
        </xsl:for-each>
        <WSKAZNIK_ZATWIERDZENIA>N</WSKAZNIK_ZATWIERDZENIA>
        <xsl:for-each select="SPRZEDAWCA">
          <xsl:for-each select="SPRZEDAWCA_ROW">
            <SPRZEDAWCA>
              <xsl:for-each select="SYMBOL">
                <SYMBOL>
                  <xsl:value-of select="."/>
                </SYMBOL>
              </xsl:for-each>
              <xsl:for-each select="NAZWA">
                <NAZWA>
                  <xsl:value-of select="."/>
                </NAZWA>
              </xsl:for-each>
              <xsl:for-each select="SKROT">
                <SKROT>
                  <xsl:value-of select="."/>
                </SKROT>
              </xsl:for-each>
              <xsl:for-each select="NIP">
                <NIP>
                  <xsl:value-of select="."/>
                </NIP>
              </xsl:for-each>
              <ADRES>
                <xsl:for-each select="MIEJSCOWOSC">
                  <MIEJSCOWOSC>
                    <xsl:value-of select="."/>
                  </MIEJSCOWOSC>
                </xsl:for-each>
                <xsl:for-each select="ULICA">
                  <ULICA>
                    <xsl:value-of select="."/>
                  </ULICA>
                </xsl:for-each>
                <xsl:for-each select="KOD_POCZTOWY">
                  <KOD_POCZTOWY>
                    <xsl:value-of select="."/>
                  </KOD_POCZTOWY>
                </xsl:for-each>
                <xsl:for-each select="NR_DOMU">
                  <NR_DOMU>
                    <xsl:value-of select="."/>
                  </NR_DOMU>
                </xsl:for-each>
                <xsl:for-each select="NR_LOKALU">
                  <NR_LOKALU>
                    <xsl:value-of select="."/>
                  </NR_LOKALU>
                </xsl:for-each>
              </ADRES>
            </SPRZEDAWCA>
          </xsl:for-each>
        </xsl:for-each>
        <xsl:for-each select="PLATNIK">
          <xsl:for-each select="PLATNIK_ROW">
            <PLATNIK>
              <xsl:for-each select="SYMBOL">
                <SYMBOL>
                  <xsl:value-of select="."/>
                </SYMBOL>
              </xsl:for-each>
              <xsl:for-each select="NAZWA">
                <NAZWA>
                  <xsl:value-of select="."/>
                </NAZWA>
              </xsl:for-each>
              <xsl:for-each select="SKROT">
                <SKROT>
                  <xsl:value-of select="."/>
                </SKROT>
              </xsl:for-each>
              <xsl:for-each select="NIP">
                <NIP>
                  <xsl:value-of select="."/>
                </NIP>
              </xsl:for-each>
              <ADRES>
                <xsl:for-each select="MIEJSCOWOSC">
                  <MIEJSCOWOSC>
                    <xsl:value-of select="."/>
                  </MIEJSCOWOSC>
                </xsl:for-each>
                <xsl:for-each select="ULICA">
                  <ULICA>
                    <xsl:value-of select="."/>
                  </ULICA>
                </xsl:for-each>
                <xsl:for-each select="KOD_POCZTOWY">
                  <KOD_POCZTOWY>
                    <xsl:value-of select="."/>
                  </KOD_POCZTOWY>
                </xsl:for-each>
                <xsl:for-each select="NR_DOMU">
                  <NR_DOMU>
                    <xsl:value-of select="."/>
                  </NR_DOMU>
                </xsl:for-each>
                <xsl:for-each select="NR_LOKALU">
                  <NR_LOKALU>
                    <xsl:value-of select="."/>
                  </NR_LOKALU>
                </xsl:for-each>
              </ADRES>
            </PLATNIK>
          </xsl:for-each>
        </xsl:for-each>
        <xsl:for-each select="ODBIORCA">
          <xsl:for-each select="ODBIORCA_ROW">
            <ODBIORCA>
              <xsl:for-each select="SYMBOL">
                <SYMBOL>
                  <xsl:value-of select="."/>
                </SYMBOL>
              </xsl:for-each>
              <xsl:for-each select="NAZWA">
                <NAZWA>
                  <xsl:value-of select="."/>
                </NAZWA>
              </xsl:for-each>
              <xsl:for-each select="SKROT">
                <SKROT>
                  <xsl:value-of select="."/>
                </SKROT>
              </xsl:for-each>
              <xsl:for-each select="NIP">
                <NIP>
                  <xsl:value-of select="."/>
                </NIP>
              </xsl:for-each>
              <ADRES>
                <xsl:for-each select="MIEJSCOWOSC">
                  <MIEJSCOWOSC>
                    <xsl:value-of select="."/>
                  </MIEJSCOWOSC>
                </xsl:for-each>
                <xsl:for-each select="ULICA">
                  <ULICA>
                    <xsl:value-of select="."/>
                  </ULICA>
                </xsl:for-each>
                <xsl:for-each select="KOD_POCZTOWY">
                  <KOD_POCZTOWY>
                    <xsl:value-of select="."/>
                  </KOD_POCZTOWY>
                </xsl:for-each>
                <xsl:for-each select="NR_DOMU">
                  <NR_DOMU>
                    <xsl:value-of select="."/>
                  </NR_DOMU>
                </xsl:for-each>
                <xsl:for-each select="NR_LOKALU">
                  <NR_LOKALU>
                    <xsl:value-of select="."/>
                  </NR_LOKALU>
                </xsl:for-each>
              </ADRES>
            </ODBIORCA>
          </xsl:for-each>
        </xsl:for-each>
        <POLA_DODATKOWE>
          <xsl:for-each select="SR_PARTY_DESCRIPTION">
            <PA_POLE_DODATKOWE_T>
              <NAZWA>ATRYBUT_T08</NAZWA>
              <WARTOSC>
                <xsl:value-of select="."/>
              </WARTOSC>
            </PA_POLE_DODATKOWE_T>
          </xsl:for-each>
        </POLA_DODATKOWE>
        <POZYCJE>
          <xsl:for-each select="ITEMS">
            <xsl:for-each select="ITEMS_ROW">
              <LG_ZASI_T>
                <xsl:for-each select="GUID">
                  <GUID_POZYCJI>
                    <xsl:value-of select="."/>
                  </GUID_POZYCJI>
                </xsl:for-each>
                <xsl:for-each select="ITEM_NUM">
                  <LP>
                    <xsl:value-of select="."/>
                  </LP>
                </xsl:for-each>
                <INDEKS>
                  <xsl:for-each select="SELLER_ITEM_ID">
                    <INDEKS>
                      <xsl:value-of select="."/>
                    </INDEKS>
                  </xsl:for-each>
                  <xsl:for-each select="NAME">
                    <NAZWA>
                      <xsl:value-of select="."/>
                    </NAZWA>
                  </xsl:for-each>
                </INDEKS>
                <xsl:for-each select="INMA_STVA_CODE">
                  <KOD_STAWKI_VAT>
                    <xsl:value-of select="."/>
                  </KOD_STAWKI_VAT>
                </xsl:for-each>
                <xsl:for-each select="QUANTITY_VALUE">
                  <ILOSC>
                    <xsl:value-of select="."/>
                  </ILOSC>
                </xsl:for-each>
                <xsl:for-each select="CURRENCY">
                  <KOD_WALUTY>
                    <xsl:value-of select="."/>
                  </KOD_WALUTY>
                </xsl:for-each>
                <xsl:for-each select="UNIT_PRICE_VALUE">
                  <CENA>
                    <xsl:value-of select="."/>
                  </CENA>
                </xsl:for-each>
                <xsl:for-each select="UNIT_PRICE_BASE">
                  <CENA_Z_CENNIKA>
                    <xsl:value-of select="."/>
                  </CENA_Z_CENNIKA>
                </xsl:for-each>
                <xsl:for-each select="UNIT_PRICE_BASE">
                  <CENA_Z_CENNIKA_WAL>
                    <xsl:value-of select="."/>
                  </CENA_Z_CENNIKA_WAL>
                </xsl:for-each>
                <xsl:for-each select="DISCOUNT_VALUE">
                  <OPUST_NA_POZYCJI>
                    <xsl:value-of select="."/>
                  </OPUST_NA_POZYCJI>
                </xsl:for-each>
                <POLA_DODATKOWE>
                  <xsl:for-each select="PROMOTION_CODE">
                    <PA_POLE_DODATKOWE_T>
                      <NAZWA>ATRYBUT_T01</NAZWA>
                      <WARTOSC>
                        <xsl:value-of select="."/>
                      </WARTOSC>
                    </PA_POLE_DODATKOWE_T>
                  </xsl:for-each>
                  <xsl:for-each select="PROMOTION_NAME">
                    <PA_POLE_DODATKOWE_T>
                      <NAZWA>ATRYBUT_T02</NAZWA>
                      <WARTOSC>
                        <xsl:value-of select="."/>
                      </WARTOSC>
                    </PA_POLE_DODATKOWE_T>
                  </xsl:for-each>
                  <xsl:for-each select="DESCRIPTION">
                    <PA_POLE_DODATKOWE_T>
                      <NAZWA>ATRYBUT_T03</NAZWA>
                      <WARTOSC>
                        <xsl:value-of select="."/>
                      </WARTOSC>
                    </PA_POLE_DODATKOWE_T>
                  </xsl:for-each>
                </POLA_DODATKOWE>
                <xsl:for-each select="UNIT_OF_MEASURE">
                  <NAZWA_JEDNOSTKI_MIARY>
                    <xsl:value-of select="."/>
                  </NAZWA_JEDNOSTKI_MIARY>
                </xsl:for-each>
              </LG_ZASI_T>
            </xsl:for-each>
          </xsl:for-each>
        </POZYCJE>
      </xsl:for-each>
    </LG_ZASP_T>
  </xsl:template>
</xsl:stylesheet>';

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
    VALUES (jg_sqre_seq.NEXTVAL,
            'ORDER',
            v_order_clob,
            v_xslt,
            'OUT/orders',
            'T',
            'IN');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'RESERVATIONS',
                   'SELECT zare.dest_symbol order_id,
                         zare.data_realizacji realization_date,
                         inma.indeks commoditiy_id,
                         jg_output_sync.format_number(zare.ilosc, 4) quantity_ordered,
                         jg_output_sync.format_number(reze.ilosc_zarezerwowana, 100) quantity_reserved
                    FROM lg_rzm_rezerwacje         reze,
                         lg_rzm_zadania_rezerwacji zare,
                         ap_indeksy_materialowe    inma
                   WHERE     reze.zare_id = zare.id
                         AND zare.inma_id = inma.id
                         AND reze.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:template match="@*|node()">
                        <xsl:copy>
                           <xsl:apply-templates select="@*|node()" />
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <RESERVATION><xsl:apply-templates/></RESERVATION>
                     </xsl:template>
                     <xsl:template priority="2" match="RESERVATIONS/RESERVATION">
                        <RESERVATION><xsl:apply-templates/></RESERVATION>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/reservations',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'SETS_COMPONENTS',
                   'SELECT inma_kpl.indeks set_id,
       inma_kpl.nazwa set_name,
       jg_output_sync.format_number (
           lg_stm_sgpu_sql.stan_goracy (inma_kpl.id,
                                        inma_kpl.jdmr_nazwa,
                                        NULL),
           100)
           available_stock,
       jg_output_sync.format_number (inma_kpl.atrybut_n05, 4)
           price_before_discount,
       jg_output_sync.format_number (inma_kpl.atrybut_n06, 4)
           price_after_discount,
       inma_kpl.atrybut_d01 valid_date,
       inma_kpl.aktualny up_to_date,
       CURSOR (
           SELECT inma_skpl.indeks commodity_id,
                  inma_skpl.nazwa commodity_name,
                  jg_output_sync.format_number (kpsk1.ilosc, 100) quantity,
                  kpsk1.premiowy bonus,
                  DECODE (kpsk1.dynamiczny, ''T'', ''DYNAMIC'', ''STATIC'')
                      set_type,
                  DECODE (inma_skpl.atrybut_t03, ''T'', ''N'', ''Y'')
                      contract_payment,
                  inma_skpl.aktualny up_to_date,
                  CURSOR (
                      SELECT indeks commodity_id,
                             nazwa commodity_name,
                             inma.aktualny up_to_date
                        FROM ap_indeksy_materialowe inma
                       WHERE inma.id IN (SELECT /*+ DYNAMIC_SAMPLING(a, 5) */
                                                COLUMN_VALUE
                                           FROM TABLE (
                                                    jg_dynamic_set_commponents (
                                                        kpsk1.id)) a))
                      dynamic_components
             FROM lg_kpl_skladniki_kompletu kpsk1,
                  ap_indeksy_materialowe inma_skpl
            WHERE     kpsk1.skl_inma_id = inma_skpl.id
                  AND kpsk1.kpl_inma_id = inma_kpl.id)
           components
  FROM ap_indeksy_materialowe inma_kpl
 WHERE inma_kpl.id IN ( :p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <SET_COMPONENTS><xsl:apply-templates/></SET_COMPONENTS>
                     </xsl:template>
                     <xsl:template priority="2" match="COMPONENTS/COMPONENTS_ROW">
                        <COMPONENT><xsl:apply-templates/></COMPONENT>
                     </xsl:template>
                     <xsl:template priority="2" match="DYNAMIC_COMPONENTS/DYNAMIC_COMPONENTS_ROW">
                        <DYNAMIC_COMPONENT><xsl:apply-templates/></DYNAMIC_COMPONENT>
                     </xsl:template>                     
                  </xsl:stylesheet>',
                   'IN/components',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'SALES_REPRESENTATIVES',
                   'SELECT okgi.id,
       osby.imie || '' '' || osby.nazwisko AS name,
       osol.atrybut_t02 AS numer_kasy,
       osol.kod AS id_erp,
       1 AS active,
       TRANSLATE (osby.imie || ''.'' || osby.nazwisko || ''.JBS'',
                  ''•π∆Ê Í£≥—Ò”ÛåúèüØø'',
                  ''AaCcEeLlNnOoSsZzZz'')
           AS userlogin,
       osby.imie username,
       osby.nazwisko usersurname,
       TRANSLATE (
           SUBSTR (osby.imie, 1, 1) || ''.'' || osby.nazwisko || ''@GOLDWELL.PL'',
           ''•π∆Ê Í£≥—Ò”ÛåúèüØø'',
           ''AaCcEeLlNnOoSsZzZz'')
           AS useremail,
       okgi.atrybut_t02 AS userphone,
       okgi.id AS area_id,
       CURSOR (
           SELECT konr.symbol customer_number
             FROM ap_kontrahenci konr,
                  lg_kontrahenci_grup kngr,
                  (SELECT *
                     FROM lg_grupy_kontrahentow
                   START WITH id = 63
                   CONNECT BY PRIOR id = grkn_id) grkn
            WHERE     kngr.konr_id = konr.id
                  AND kngr.grkn_id = grkn.id
                  AND grkn.nazwa = osol.atrybut_t01)
           contractors
  FROM lg_osoby_log osol, pa_osoby osby, ap_okregi_sprzedazy okgi
 WHERE     okgi.symbol = osol.atrybut_t01
       AND osol.id IN (:p_id)
       AND osol.osby_id = osby.id',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <SALES_REPRESENTATIVE><xsl:apply-templates/></SALES_REPRESENTATIVE>
                     </xsl:template>
                     <xsl:template priority="2" match="CONTRACTORS/CONTRACTORS_ROW">
                        <CONTRACTOR><xsl:apply-templates/></CONTRACTOR>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/sales_representatives',
                   'T',
                   'OUT');

    v_xslt :=
        '<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" encoding="windows-1250" indent="yes"/>
  <xsl:template match="/">
    <PA_KONTRAHENT_TK xmlns="http://www.teta.com.pl/teta2000/kontrahent-1" wersja="1.0">
      <xsl:for-each select="NewCustomer">
        <xsl:for-each select="BasicData">
          <xsl:for-each select="MobizID">
            <SYMBOL>
              <xsl:value-of select="."/>
            </SYMBOL>
          </xsl:for-each>
          <xsl:for-each select="Name">
            <NAZWA>
              <xsl:value-of select="."/>
            </NAZWA>
          </xsl:for-each>
          <xsl:for-each select="Shortcut">
            <SKROT>
              <xsl:value-of select="."/>
            </SKROT>
          </xsl:for-each>
          <xsl:for-each select="TIN">
            <NIP>
              <xsl:value-of select="."/>
            </NIP>
          </xsl:for-each>
          <ADRES>
            <xsl:for-each select="Address">
              <xsl:for-each select="City">
                <MIEJSCOWOSC>
                  <xsl:value-of select="."/>
                </MIEJSCOWOSC>
              </xsl:for-each>
              <xsl:for-each select="Street">
                <ULICA>
                  <xsl:value-of select="."/>
                </ULICA>
              </xsl:for-each>
              <xsl:for-each select="Postcode">
                <KOD_POCZTOWY>
                  <xsl:value-of select="."/>
                </KOD_POCZTOWY>
              </xsl:for-each>
            </xsl:for-each>
            <xsl:for-each select="Phone">
              <NR_TEL>
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </NR_TEL>
            </xsl:for-each>
            <xsl:for-each select="Fax">
              <NR_FAX>
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </NR_FAX>
            </xsl:for-each>
            <ADRESY_EMAIL>
              <xsl:for-each select="Email">
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </xsl:for-each>
            </ADRESY_EMAIL>
            <RegionID>080</RegionID>
            <ProvinceID>450</ProvinceID>
          </ADRES>
          <ClassID>Detal</ClassID>
          <Profile>Reseller</Profile>
          <ContactPerson>Tomasz Wspania≥y</ContactPerson>
          <ChainID>123</ChainID>
        </xsl:for-each>
        <AdditionalData>
          <SalesRepresentativeID>5235</SalesRepresentativeID>
        </AdditionalData>
        <PLATNIK_VAT>T</PLATNIK_VAT>
        <BLOKADA_ZAKUPU>N</BLOKADA_ZAKUPU>
        <RODZAJ_DATY_WAR_HANDL_FAKT>S</RODZAJ_DATY_WAR_HANDL_FAKT>
        <RODZAJ_DATY_WAR_HANDL_ZAM>W</RODZAJ_DATY_WAR_HANDL_ZAM>
        <RODZAJ_DATY_TERM_PLAT_FS>DW</RODZAJ_DATY_TERM_PLAT_FS>
        <GRUPY_KONTRHENTA/>
        <JEDNOSTKI_OSOBY/>
      </xsl:for-each>
    </PA_KONTRAHENT_TK>
  </xsl:template>
</xsl:stylesheet>';

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
    VALUES (jg_sqre_seq.NEXTVAL,
            'NEW_CONTRACTORS',
            'SELECT osol.kod        id,
                         osby.code       name,
                         osol.aktualna   active,
                         osol.first_name username,
                         osol.surname    usersurname,
                         CURSOR (SELECT konr.symbol customerid
                                   FROM lg_osoby_log osol1,
                                        (SELECT *
                                           FROM lg_grupy_kontrahentow
                                          START WITH id = 63
                                        CONNECT BY PRIOR id = grkn_id) grko,
                                        lg_kontrahenci_grup kngr,
                                        ap_kontrahenci konr
                                  WHERE     osol1.atrybut_t01 = grko.nazwa
                                        AND grko.id = kngr.grkn_id
                                        AND osol1.aktualna = ''T''
                                        AND kngr.konr_id = konr.id
                                        AND osol1.id = osol.id) customers
                     FROM lg_osoby_log osol, pa_osoby osby
                    WHERE     osol.atrybut_t01 IS NOT NULL
                          AND osol.osby_id = osby.id
                          AND osol.id IN (:p_id)',
            v_xslt,
            'OUT/new_customer',
            'T',
            'IN');

    v_xslt :=
        '<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" encoding="windows-1250" indent="yes"/>
  <xsl:template match="/">
    <PA_KONTRAHENT_TK xmlns="http://www.teta.com.pl/teta2000/kontrahent-1" wersja="1.0">
      <xsl:for-each select="CustomerData">
        <xsl:for-each select="BasicData">
          <xsl:for-each select="MobizID">
            <SYMBOL>
              <xsl:value-of select="."/>
            </SYMBOL>
          </xsl:for-each>
          <xsl:for-each select="Name">
            <NAZWA>
              <xsl:value-of select="."/>
            </NAZWA>
          </xsl:for-each>
          <xsl:for-each select="Shortcut">
            <SKROT>
              <xsl:value-of select="."/>
            </SKROT>
          </xsl:for-each>
          <xsl:for-each select="TIN">
            <NIP>
              <xsl:value-of select="."/>
            </NIP>
          </xsl:for-each>
          <ADRES>
            <xsl:for-each select="Address">
              <xsl:for-each select="City">
                <MIEJSCOWOSC>
                  <xsl:value-of select="."/>
                </MIEJSCOWOSC>
              </xsl:for-each>
              <xsl:for-each select="Street">
                <ULICA>
                  <xsl:value-of select="."/>
                </ULICA>
              </xsl:for-each>
              <xsl:for-each select="Postcode">
                <KOD_POCZTOWY>
                  <xsl:value-of select="."/>
                </KOD_POCZTOWY>
              </xsl:for-each>
            </xsl:for-each>
            <xsl:for-each select="Phone">
              <NR_TEL>
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </NR_TEL>
            </xsl:for-each>
            <xsl:for-each select="Fax">
              <NR_FAX>
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </NR_FAX>
            </xsl:for-each>
            <ADRESY_EMAIL>
              <xsl:for-each select="Email">
                <VARCHAR2>
                  <xsl:value-of select="."/>
                </VARCHAR2>
              </xsl:for-each>
            </ADRESY_EMAIL>
            <RegionID>080</RegionID>
            <ProvinceID>450</ProvinceID>
          </ADRES>
          <ClassID>Detal</ClassID>
          <Profile>Reseller</Profile>
          <ContactPerson>Tomasz Wspania≥y</ContactPerson>
          <ChainID>123</ChainID>
        </xsl:for-each>
        <AdditionalData>
          <SalesRepresentativeID>5235</SalesRepresentativeID>
        </AdditionalData>
        <PLATNIK_VAT>T</PLATNIK_VAT>
        <BLOKADA_ZAKUPU>N</BLOKADA_ZAKUPU>
        <RODZAJ_DATY_WAR_HANDL_FAKT>S</RODZAJ_DATY_WAR_HANDL_FAKT>
        <RODZAJ_DATY_WAR_HANDL_ZAM>W</RODZAJ_DATY_WAR_HANDL_ZAM>
        <RODZAJ_DATY_TERM_PLAT_FS>DW</RODZAJ_DATY_TERM_PLAT_FS>
        <GRUPY_KONTRHENTA/>
        <JEDNOSTKI_OSOBY/>
      </xsl:for-each>
    </PA_KONTRAHENT_TK>
  </xsl:template>
</xsl:stylesheet>';

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
    VALUES (jg_sqre_seq.NEXTVAL,
            'CUSTOMER_DATA',
            'SELECT osol.kod        id,
                         osby.code       name,
                         osol.aktualna   active,
                         osol.first_name username,
                         osol.surname    usersurname,
                         CURSOR (SELECT konr.symbol customerid
                                   FROM lg_osoby_log osol1,
                                        (SELECT *
                                           FROM lg_grupy_kontrahentow
                                          START WITH id = 63
                                        CONNECT BY PRIOR id = grkn_id) grko,
                                        lg_kontrahenci_grup kngr,
                                        ap_kontrahenci konr
                                  WHERE     osol1.atrybut_t01 = grko.nazwa
                                        AND grko.id = kngr.grkn_id
                                        AND osol1.aktualna = ''T''
                                        AND kngr.konr_id = konr.id
                                        AND osol1.id = osol.id) customers
                     FROM lg_osoby_log osol, pa_osoby osby
                    WHERE     osol.atrybut_t01 IS NOT NULL
                          AND osol.osby_id = osby.id
                          AND osol.id IN (:p_id)',
            v_xslt,
            'OUT/new_customer',
            'T',
            'IN');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'DELIVERY_METHODS',
                   'SELECT kod delivery_method_code,
                         opis description,
                         aktualna up_to_date
                    FROM ap_sposoby_dostaw spdo
                   WHERE spdo.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                  <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                  <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <DELIVERY_METHOD><xsl:apply-templates /></DELIVERY_METHOD>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/delivery_methods',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'PAYMENTS_METHODS',
                   'SELECT foza.kod payment_method_code,
                         foza.opis description,
                         odroczenie_platnosci deferment_of_payment,
                         (SELECT rv_meaning
                            FROM cg_ref_codes
                           WHERE     rv_domain = ''FORMY_ZAPLATY''
                                 AND rv_low_value = foza.typ) payment_type,
                          aktualna up_to_date
                    FROM ap_formy_zaplaty foza
                   WHERE foza.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <PAYMENT_METHOD><xsl:apply-templates /></PAYMENT_METHOD>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/payments_methods',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'ORDERS_PATTERNS',
                   'SELECT wzrc.pattern pattern_code,
                         wzrc.name pattern_name,
                         wzrc.up_to_date
                    FROM lg_documents_templates wzrc
                   WHERE     document_type = ''ZS''
                         AND wzrc.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <ORDER_PATTERN><xsl:apply-templates /></ORDER_PATTERN>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/orders_patterns',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'DISCOUNTS',
                   'WITH upta
     AS (SELECT upta.symbol,
                wakw.id AS wakw_id,
                upta.id AS upta_id,
                krwy.typ_wykorzystania
           FROM lg_upusty_tabelaryczne upta
                JOIN lg_kryteria_wykorzystane krwy ON upta.id = krwy.upta_id
                JOIN lg_wartosci_kryt_wyk wakw ON wakw.krwy_id = krwy.id
          WHERE upta.symbol LIKE ''UG%''),
     upta_koup
     AS (SELECT a.upta_id, koup.upust_procentowy
           FROM (SELECT *
                   FROM upta
                        PIVOT
                            (MAX (wakw_id) wakw_id
                            FOR typ_wykorzystania
                            IN (''W'' AS "W", ''K'' AS "K"))) a
                JOIN lg_komorki_upustow koup
                    ON     koup.wakw_id_kolumna = a.k_wakw_id
                       AND koup.wakw_id_wiersz = a.w_wakw_id)
SELECT upta.symbol discount_number,
       inma.indeks item_index,
       konr.symbol customer_number,
       gras.kod commodity_group_code,
       grod.grupa reciever_group,
       prup.data_od date_from,
       NVL (prup.data_do, TO_DATE (''2049/12/31'', ''YYYY/MM/DD'')) date_to,
       jg_output_sync.format_number (
           NVL (upko.upust_procentowy, prup.upust_procentowy),
           100)
           percent_discount
  FROM lg_przyp_upustow prup
       INNER JOIN lg_upusty_tabelaryczne upta ON prup.upta_id = upta.id
       LEFT JOIN ap_indeksy_materialowe inma ON inma.id = prup.inma_id
       LEFT JOIN ap_kontrahenci konr ON konr.id = prup.konr_id
       LEFT JOIN ap_grupy_asortymentowe gras ON gras.id = prup.gras_id
       LEFT JOIN ap_grupy_odbiorcow grod ON grod.id = prup.grod_id
       LEFT JOIN upta_koup upko ON prup.upta_id = upko.upta_id
 WHERE     (   prup.upust_procentowy IS NOT NULL
            OR upko.upust_procentowy IS NOT NULL)
       AND SYSDATE BETWEEN prup.data_od AND NVL (data_do, SYSDATE)
       AND prup.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <DISCOUNT><xsl:apply-templates/></DISCOUNT>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/discounts',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'WAREHOUSES',
                   'SELECT maga.kod id,
       maga.nazwa name,
       CURSOR (
           SELECT inma1.indeks commodity_id,
                  jg_output_sync.format_number (sum(stma1.stan_goracy), 100)
                      quantity
             FROM ap_stany_magazynowe stma1,
                  ap_indeksy_materialowe inma1,
                  ap_magazyny maga1
            WHERE     inma1.id = stma1.suob_inma_id
                  AND maga1.id = stma1.suob_maga_id
                  AND stma1.suob_inma_id in (SELECT suob_inma_id from ap_stany_magazynowe stma where stma.id IN (:p_id))
                  AND maga1.kod = maga.kod
                  AND maga1.id in (SELECT suob_maga_id from ap_stany_magazynowe stma where stma.id IN (:p_id))
                  group by inma1.indeks 
                  )
           stocks
  FROM ap_stany_magazynowe stma, ap_magazyny maga
WHERE     stma.suob_maga_id = maga.id
       AND (kod LIKE ''1__'' OR kod in (''500'',''300''))
       AND stma.suob_inma_id in (SELECT suob_inma_id from ap_stany_magazynowe stma where stma.id IN (:p_id))
       AND stma.suob_maga_id in (SELECT suob_maga_id from ap_stany_magazynowe stma where stma.id IN (:p_id))
GROUP BY maga.kod, maga.nazwa',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                    <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                    <xsl:strip-space elements="*"/>
                    <xsl:template match="node()|@*">
                       <xsl:copy>
                          <xsl:apply-templates select="node()|@*"/>
                       </xsl:copy>
                    </xsl:template>
                    <xsl:template match="*[not(@*|comment()|processing-instruction()) and normalize-space()='''']"/>
                    <xsl:template priority="2" match="ROW">
                       <WAREHOUSE><xsl:apply-templates/></WAREHOUSE>
                    </xsl:template>
                    <xsl:template priority="2" match="STOCKS/STOCKS_ROW">
                       <STOCK><xsl:apply-templates/></STOCK>
                    </xsl:template>
                 </xsl:stylesheet>',
                   'IN/warehouses',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'CONTRACTS',
                   'SELECT umsp.symbol id,
       konr.symbol contractor_id,
       umsp1.date_from,
       umsp.data_do date_to,
       wzrc.nazwa contract_destination,
       jg_output_sync.format_number (umsp1.contract_value, 10) contract_value,
       jg_output_sync.format_number (
          umsp1.splata,
           10)
           contract_value_realized,
       CASE
           WHEN   umsp1.splata
                -   (umsp1.contract_value / umsp1.duration)
                  * (FLOOR (MONTHS_BETWEEN (SYSDATE, umsp1.date_from))) < 0
           THEN
              ''T''
           ELSE
               ''N''
       END
           delayed,
       CASE
           WHEN     (umsp1.contract_value / umsp1.duration)
                  * months_passed
                - umsp1.splata > (umsp1.contract_value / umsp1.duration) * 3
           THEN
               ''N''
           ELSE
               ''T''
       END
           can_skip_repayment,
  round(umsp1.contract_value/umsp1.duration,2) as monthly_installment,
  round(greatest(umsp1.splata-umsp1.contract_value,least(0,umsp1.splata - umsp1.contract_value/umsp1.duration*months_passed)),2) debt
  FROM lg_ums_umowy_sprz umsp,
       ap_kontrahenci konr,
       lg_wzorce wzrc,
       (SELECT id,
                (SELECT Lg_Ums_Umsi_Def.Wartosc(UMSI.ID)
                  FROM 
                       lg_ums_umowy_sprz_it umsi
                 WHERE umsi.umsp_id = umsp.id)
                   contract_value,
               (SELECT NVL (SUM (umru.wartosc), 0)
                  FROM lg_ums_realizacje_umsi umru, lg_ums_umowy_sprz_it umsi
                 WHERE umsi.id = umru.uiwl_id AND umsi.umsp_id = umsp.id)
                   splata,
               NVL (ADD_MONTHS (umsp.data_do, -umsp.atrybut_n01) + 1,
                    umsp.data_od)
                   AS date_from,
               nvl(umsp.atrybut_n01,round(months_between(umsp.data_do,umsp.data_od))) duration,
               floor(months_between(sysdate,NVL (ADD_MONTHS (umsp.data_do, -umsp.atrybut_n01) + 1,
                    umsp.data_od))) as months_passed
          FROM lg_ums_umowy_sprz umsp
          WHERE umsp.data_wystawienia >= to_date(''2016/01/01'',''YYYY/MM/DD'')
          AND umsp.zamknieta = ''N''
          ) umsp1
 WHERE konr.id = umsp.konr_id_pl AND wzrc.id = umsp.wzrc_id AND umsp.id = umsp1.id  AND umsp.id IN ( :p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:template match="@*|node()">
                        <xsl:copy>
                           <xsl:apply-templates select="@*|node()" />
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <CONTRACT><xsl:apply-templates/></CONTRACT>            
                     </xsl:template>
                     <xsl:template priority="2" match="LINES/LINES_ROW">
                        <LINE><xsl:apply-templates/></LINE>            
                     </xsl:template>
                     <xsl:template priority="2" match="PERIODS/PERIODS_ROW">
                        <PERIOD><xsl:apply-templates/></PERIOD>            
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/contracts',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'SUPPORT_FUNDS',
                   'SELECT fwk.konr_symbol AS client_symbol,
       SUM (fwk.fwk_m_pozostalo) AS marketing_support_fund,
       SUM (fwk.fwk_t_pozostalo) AS real_support_fund,
       SUM (fwk.fwk_m_pozostalo) + SUM (fwk.fwk_t_pozostalo)
           AS sum_support_fund
  FROM jbs_mp_przeglad_fwk fwk
 WHERE     fwk.data_faktury >= ADD_MONTHS (TRUNC (SYSDATE, ''MM''), -12)
       --AND fwk.czy_zaplacona = ''T''
       AND fwk.konr_symbol IN (SELECT konr_symbol
                                 FROM jbs_mp_przeglad_fwk
                                WHERE id IN (:p_id))
GROUP BY fwk.konr_symbol',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <SUPPORT_FUND><xsl:apply-templates/></SUPPORT_FUND>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/support_funds',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'LOYALITY_POINTS',
                   'SELECT puko.CLIENT_SYMBOL,
                         puko.POINTS_TYPE,
                         puko.POINTS_VALUE,
                         puko.SUM_REAL_POINTS_VALUE,
                         puko.SUM_TEMPORARY_POINTS_VALUE,
                         puko.SUM_POINTS_VALUE,
                         puko.CALCULATION_DATE,
                         ADD_MONTHS(puko.CALCULATION_DATE, 24) EXPIRE_DATE,
                         CURSOR (SELECT DECODE(puko1.rzeczywiste, ''T'', ''RZECZYWISTE'', ''TYMCZASOWE'') AS POINTS_TYPE,
                                        puko1.wartosc_punktow             AS POINTS_VALUE,
                                        dosp.data_faktury                 AS CALCULATION_DATE,
                                        ADD_MONTHS(dosp.data_faktury, 24) AS EXPIRE_DATE
                                   FROM lg_plo_punkty_kontrahenta puko1,
                                        lg_dokumenty_sprz_vw dosp
                                  WHERE     dosp.id = puko1.dosp_id
                                        AND puko1.konr_id = puko.konr_id
                               ORDER BY puko1.id DESC)                    AS HISTORY
                    FROM (SELECT konr.symbol  AS CLIENT_SYMBOL,
                                 DECODE(puko.rzeczywiste, ''T'', ''RZECZYWISTE'', ''TYMCZASOWE'') AS POINTS_TYPE,
                                 puko.wartosc_punktow AS POINTS_VALUE,
                                 (SELECT SUM(puko1.wartosc_punktow)
                                    FROM lg_plo_punkty_kontrahenta puko1
                                   WHERE     puko1.konr_id = puko.konr_id
                                         AND puko1.rzeczywiste = ''T'') AS SUM_REAL_POINTS_VALUE,
                                 (SELECT SUM(puko1.wartosc_punktow)
                                    FROM lg_plo_punkty_kontrahenta puko1
                                   WHERE     puko1.konr_id = puko.konr_id
                                         AND puko1.rzeczywiste = ''N'') AS SUM_TEMPORARY_POINTS_VALUE, 
                                 (SELECT SUM(puko1.wartosc_punktow)
                                    FROM lg_plo_punkty_kontrahenta puko1
                                   WHERE puko1.konr_id = puko.konr_id) AS SUM_POINTS_VALUE,                
                                 NVL((SELECT CASE WHEN (SELECT MAX(id) FROM lg_plo_punkty_kontrahenta WHERE konr_id = puko.konr_id) = dosp.puko_id THEN dosp.data_faktury ELSE dosp.data_faktury + 1 END
                                        FROM (SELECT puko1.id puko_id,
                                                     puko1.konr_id konr_id,
                                                     dosp.data_faktury data_faktury                                     
                                                FROM lg_plo_punkty_kontrahenta puko1,
                                                     lg_dokumenty_sprz_vw dosp
                                               WHERE     dosp.id = puko1.dosp_id
                                                     AND puko1.dosp_id IS NOT NULL
                                            ORDER BY puko1.id desc) dosp
                                       WHERE     dosp.konr_id = puko.konr_id
                                             AND dosp.data_faktury IS NOT NULL
                                             AND ROWNUM = 1), TO_DATE(''01-01-2000'', ''DD-MM-YYYY'')) CALCULATION_DATE,
                                 puko.konr_id
                           FROM lg_plo_punkty_kontrahenta puko,
                                ap_kontrahenci konr
                          WHERE     konr.id = puko.konr_id
                                AND puko.id IN (:p_id)) puko',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <LOYALITY_POINT><xsl:apply-templates/></LOYALITY_POINT>
                     </xsl:template>
                     <xsl:template priority="2" match="HISTORY/HISTORY_ROW">
                        <POINTS><xsl:apply-templates/></POINTS>            
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/loyality_points',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'TRADE_CONTRACTS_INDIVIDUAL',
                   'SELECT konr.symbol contractors_id, 
        konr.nr_umowy_ind AS contract_number,
       DECODE (individual_contract,
               ''T'', konr.data_umowy_ind,
               konr.atrybut_d01)
           AS contract_date,
       individual_contract AS individual_contract,
       konr.foza_kod AS default_payment_type,
       NVL (konr.limit_kredytowy, 0) AS credit_limit,
       konr.dni_do_zaplaty AS payment_date,
       prup.upust_procentowy AS discount_percent,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 0) AS quarter_points,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 1) AS half_year_points,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 2) AS year_points,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 3) AS quarter_discount,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 4) AS half_year_discount,
       a_mp_dekoduj_pkt (konr.atrybut_t07, 5) AS year_discount,
       konr.atrybut_n05 AS quarter_threshold,
       konr.atrybut_n02 AS half_year_threshold,
       konr.atrybut_n03 AS year_threshold,
       DECODE (
           (SELECT COUNT (*)
              FROM lg_przyp_upustow prup1
                   JOIN lg_upusty_tabelaryczne upta1
                       ON prup1.upta_id = upta1.id
             WHERE     upta1.symbol = ''SKONTO''
                   AND SYSDATE BETWEEN prup1.data_od
                                   AND NVL (prup1.data_do, SYSDATE)
                   AND prup.konr_id = konr.id),
           0, ''N'',
           ''T'')
           skonto
  FROM (SELECT CASE
                   WHEN konr.atrybut_t05 LIKE ''%UM IND%'' THEN ''T''
                   ELSE ''N''
               END
                   individual_contract,
               konr.*
          FROM ap_kontrahenci konr) konr,
       lg_przyp_upustow prup
 WHERE prup.grod_id(+) = konr.grod_id AND konr.id IN ( :p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <TRADE_CONTRACTS><xsl:apply-templates/></TRADE_CONTRACTS>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/trade_contracts',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'TRADE_CONTRACTS',
                   'WITH ind_co
     AS (SELECT konr.id AS konr_id,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 0),
                        atrybut_n04)
                    AS quarter_points,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 1),
                        NULL)
                    AS half_year_points,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 2),
                        NULL)
                    AS year_points,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 3),
                        NULL)
                    AS quarter_discount,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 4),
                        NULL)
                    AS half_year_discount,
                DECODE (individual_contract,
                        ''T'', a_mp_dekoduj_pkt (konr.atrybut_t07, 5),
                        NULL)
                    AS year_discount,
                DECODE (individual_contract, ''T'', konr.atrybut_n05, NULL)
                    AS quarter_threshold,
                DECODE (individual_contract, ''T'', konr.atrybut_n02, NULL)
                    AS half_year_threshold,
                DECODE (individual_contract, ''T'', konr.atrybut_n03, NULL)
                    AS year_threshold
           FROM (SELECT CASE
                            WHEN konr.atrybut_t05 LIKE ''%UM IND%'' THEN ''T''
                            ELSE ''N''
                        END
                            individual_contract,
                        konr.*
                   FROM ap_kontrahenci konr
                  WHERE konr.platnik = ''T'') konr),
     ind_co_data
     AS (SELECT *
           FROM ind_co
                UNPIVOT
                    (quantity
                    FOR col_name
                    IN (quarter_points,
                       half_year_points,
                       year_points,
                       quarter_discount,
                       half_year_discount,
                       year_discount,
                       quarter_threshold,
                       half_year_threshold,
                       year_threshold)))
SELECT konr.symbol contractors_id,
       konr.nr_umowy_ind AS contract_number,
       DECODE (individual_contract,
               ''T'', konr.data_umowy_ind,
               konr.atrybut_d01)
           AS contract_date,
       individual_contract AS individual_contract,
       konr.foza_kod AS default_payment_type,
       NVL (konr.limit_kredytowy, 0) AS credit_limit,
       konr.dni_do_zaplaty AS payment_date,
       SUBSTR (grod.grupa, 2, 2) AS discount_percent,
       CURSOR (
           SELECT col_name,
                  jg_output_sync.format_number (quantity, 2) quantity
             FROM ind_co_data icd
            WHERE quantity IS NOT NULL AND icd.konr_id = konr.id)
           AS bonus_points,
       DECODE (
           (SELECT COUNT (*)
              FROM lg_przyp_upustow prup1
                   JOIN lg_upusty_tabelaryczne upta1
                       ON prup1.upta_id = upta1.id
             WHERE     upta1.symbol = ''SKONTO''
                   AND SYSDATE BETWEEN prup1.data_od
                                   AND NVL (prup1.data_do, SYSDATE)
                   AND prup1.konr_id = konr.id),
           0, ''N'',
           ''T'')
           skonto
  FROM (SELECT CASE
                   WHEN konr.atrybut_t05 LIKE ''%UM IND%'' THEN ''T''
                   ELSE ''N''
               END
                   individual_contract,
               konr.*
          FROM ap_kontrahenci konr) konr,
       ap_grupy_odbiorcow grod
 WHERE     grod.id(+) = konr.grod_id
       AND konr.platnik = ''T''
       AND konr.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <TRADE_CONTRACTS><xsl:apply-templates/></TRADE_CONTRACTS>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/trade_contracts',
                   'T',
                   'OUT');

    INSERT INTO jg_sql_repository (id,
                                   object_type,
                                   sql_query,
                                   xslt,
                                   file_location,
                                   up_to_date,
                                   direction)
        VALUES (
                   jg_sqre_seq.NEXTVAL,
                   'DELIVERIES',
                   'SELECT doob.symbol AS document_symbol,
       doob.konr_symbol AS contractor_symbol,
       doob.data_realizacji AS realization_date,
       doob.numer AS document_number,
       doob.numer_zamowienia AS order_symbol,
       (SELECT cono.tracking_number
          FROM ap_dokumenty_obrot doob1
               JOIN lg_specyf_wysylki_doob swdo ON swdo.doob_id = doob1.id
               JOIN lg_specyf_wysylki_opak spwo
                   ON spwo.spws_id = swdo.spws_id
               JOIN lg_trs_source_documents sodo ON sodo.doc_id = spwo.id
               JOIN lg_trs_sodo_shun sosh ON sosh.sodo_id = sodo.id
               JOIN lg_trs_shipping_units shun ON shun.id = sosh.shun_id
               JOIN lg_trs_consignment_notes cono
                   ON cono.id = shun.cono_id AND cono.status <> ''OP''
         WHERE doob1.id = doob.id)
           AS tracking_number,
       (SELECT cono.tracking_link
          FROM ap_dokumenty_obrot doob1
               JOIN lg_specyf_wysylki_doob swdo ON swdo.doob_id = doob1.id
               JOIN lg_specyf_wysylki_opak spwo
                   ON spwo.spws_id = swdo.spws_id
               JOIN lg_trs_source_documents sodo ON sodo.doc_id = spwo.id
               JOIN lg_trs_sodo_shun sosh ON sosh.sodo_id = sodo.id
               JOIN lg_trs_shipping_units shun ON shun.id = sosh.shun_id
               JOIN lg_trs_consignment_notes cono
                   ON cono.id = shun.cono_id AND cono.status <> ''OP''
         WHERE doob1.id = doob.id)
           AS tracking_link,
       CURSOR (SELECT dobi.numer AS ordinal,
                      dobi.inma_symbol AS item_symbol,
                      dobi.inma_nazwa AS item_name,
                      dobi.ilosc AS quantity,
                      dobi.cena AS price,
                      dobi.wartosc AS VALUE
                 FROM ap_dokumenty_obrot_it dobi
                WHERE dobi.doob_id = doob.id
               ORDER BY dobi.numer)
           AS lines
  FROM ap_dokumenty_obrot doob
 WHERE     doob.wzty_kod = ''WZ''
       AND doob.numer_zamowienia IS NOT NULL
       AND doob.id IN (:p_id)',
                   '<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                     <xsl:output method="xml" version="1.5" indent="yes" omit-xml-declaration="no" />
                     <xsl:strip-space elements="*"/>
                     <xsl:template match="node()|@*">
                        <xsl:copy>
                           <xsl:apply-templates select="node()|@*"/>
                        </xsl:copy>
                     </xsl:template>
                     <xsl:template match="*[not(@*|*|comment()|processing-instruction()) and normalize-space()='''']"/>
                     <xsl:template priority="2" match="ROW">
                        <DELIVERY><xsl:apply-templates/></DELIVERY>
                     </xsl:template>
                     <xsl:template priority="2" match="LINES/LINES_ROW">
                        <LINE><xsl:apply-templates/></LINE>
                     </xsl:template>
                  </xsl:stylesheet>',
                   'IN/deliveries',
                   'T',
                   'OUT');
END;
/

BEGIN
    lg_sql_wykonywanie.wykonaj_ddl (
        p_wyrazenie   => 'begin DBMS_SCHEDULER.DROP_JOB(job_name=> ''INTEGRACJAINFINITE''); end;',
        p_nr_bledu    => -27475);
    DBMS_SCHEDULER.create_job (
        '"INTEGRACJAINFINITE"',
        job_type              => 'PLSQL_BLOCK',
        job_action            => 'BEGIN jg_output_sync.PROCESS(); jg_input_sync.get_from_ftp(); END;',
        number_of_arguments   => 0,
        start_date            => TO_TIMESTAMP_TZ (
                                    '18-SEP-2016 12.40.32,357000000 PM +02:00',
                                    'DD-MON-RRRR HH.MI.SSXFF AM TZR',
                                    'NLS_DATE_LANGUAGE=english'),
        repeat_interval       => 'FREQ=MINUTELY; INTERVAL=10;',
        end_date              => NULL,
        job_class             => '"DEFAULT_JOB_CLASS"',
        enabled               => FALSE,
        auto_drop             => TRUE,
        comments              => NULL);
    DBMS_SCHEDULER.enable ('"INTEGRACJAINFINITE"');
    COMMIT;
END;
/


COMMIT
/