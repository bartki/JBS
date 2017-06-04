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
        v_okreg_id           ap_okregi_sprzedazy.id%TYPE;
        v_symbol             ap_kontrahenci.symbol%TYPE;
    BEGIN
        v_xml := transform_xml (p_xml => p_xml, p_object_type => p_object_type);

        v_symbol :=
            pa_xmltype.wartosc (v_xml, '/PA_KONTRAHENT_TK/SYMBOL', v_core_ns);

        IF v_symbol IS NULL
        THEN
            SELECT 'NKKX' || jbs_mp_nkk_kln.NEXTVAL
              INTO v_symbol
              FROM DUAL;
        END IF;

        v_xml :=
            xmltype.APPENDCHILDXML (
                v_xml,
                'PA_KONTRAHENT_TK',
                xmltype ('<SYMBOL>' || v_symbol || '</SYMBOL>'),
                v_core_ns);

        v_okreg_id :=
            pa_xmltype.wartosc (v_xml,
                                '/PA_KONTRAHENT_TK/SALES_REPRESENTATIVE_ID',
                                v_core_ns);

        IF v_okreg_id IS NOT NULL
        THEN
            v_xml :=
                xmltype.APPENDCHILDXML (
                    v_xml,
                    'PA_KONTRAHENT_TK',
                    xmltype (
                           '<OKREG_SPRZEDAZY>'
                        || lg_okgi_sql.rt (p_id => v_okreg_id).symbol
                        || '</OKREG_SPRZEDAZY>'),
                    v_core_ns);
        END IF;

        apix_lg_konr.update_obj (p_konr                           => v_xml.getclobval,
                                 p_update_limit                   => FALSE,
                                 p_update_addresses_by_konr_mdf   => TRUE);

        RETURN lg_konr_sql.id (p_symbol => v_symbol);
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
        r_ksks              rk_ks_kasy%ROWTYPE;
        v_konr_id           ap_kontrahenci.id%TYPE;
        v_ksrk_guid         rk_ks_raporty_kasowe.guid%TYPE;
        vr_document         api_rk_ks_ksdk.tr_document;
        vr_payment          api_rk_ks_ksdk.tr_payment;
        v_ksdk_guid         rk_ks_dokumenty_kasowe.guid%TYPE;
        v_ksdk_id           rk_ks_dokumenty_kasowe.id%TYPE;
        v_kasjer_id         rk_ks_dokumenty_kasowe.kasjer_id%TYPE;
        v_kasjer_imie       rk_ks_dokumenty_kasowe.kasjer_imie%TYPE;
        v_kasjer_nazwisko   rk_ks_dokumenty_kasowe.kasjer_nazwisko%TYPE;
        v_dosp_id           lg_sal_invoices.id%TYPE;
        r_plat              lg_dosp_platnosci%ROWTYPE;
        r_ksrk              rk_ks_raporty_kasowe%ROWTYPE;
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
                                       || ' znajduje sie już w kasie. Otrzymał symbol: '
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
            vr_document.subtype := 'KP201';



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

            v_kasjer_id := NULL;
            v_kasjer_imie := NULL;
            v_kasjer_nazwisko := NULL;

            FOR r_osob IN (SELECT prac_id, imie, nazwisko
                           FROM lg_osoby
                           WHERE atrybut_t02 = r_ksdk.cash_register_symbol)
            LOOP
                v_kasjer_id := r_osob.prac_id;
                v_kasjer_imie := r_osob.imie;
                v_kasjer_nazwisko := r_osob.nazwisko;
            END LOOP;

            UPDATE rk_ks_dokumenty_kasowe
               SET t_01 = r_ksdk.cash_receipt_number_1,
                   t_02 = r_ksdk.cash_receipt_number_2,
                   kasjer_id = v_kasjer_id,
                   kasjer_nazwisko = v_kasjer_nazwisko,
                   kasjer_imie = v_kasjer_imie
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

