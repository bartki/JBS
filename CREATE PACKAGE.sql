CREATE OR REPLACE PACKAGE jg_ftp AS
------------------------------------------------------------------------------------------------------------------------
    TYPE t_string_table IS TABLE OF VARCHAR2 (32767);

------------------------------------------------------------------------------------------------------------------------
    FUNCTION Login (
        p_host                          IN      VARCHAR2,
        p_port                          IN      VARCHAR2,
        p_user                          IN      VARCHAR2,
        p_pass                          IN      VARCHAR2,
        p_timeout                       IN      NUMBER := NULL)
        RETURN UTL_TCP.connection;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION Get_Passive (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection)
        RETURN UTL_TCP.connection;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE LOGOUT (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_reply                         IN      BOOLEAN := TRUE);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Send_Command (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_command                       IN      VARCHAR2,
        p_reply                         IN      BOOLEAN := TRUE);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Get_Reply (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection);

------------------------------------------------------------------------------------------------------------------------
    FUNCTION Get_Local_Ascii_Data (
        p_dir                           IN      VARCHAR2,
        p_file                          IN      VARCHAR2)
        RETURN CLOB;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION Get_Local_Binary_Data (
        p_dir                           IN      VARCHAR2,
        p_file                          IN      VARCHAR2)
        RETURN BLOB;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION Get_Remote_Ascii_Data (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_file                          IN      VARCHAR2)
        RETURN CLOB;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION Get_Remote_Binary_Data (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_file                          IN      VARCHAR2)
        RETURN BLOB;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Put_Local_Ascii_Data (
        p_data                          IN      CLOB,
        p_dir                           IN      VARCHAR2,
        p_file                          IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Put_Local_Binary_Data (
        p_data                          IN      BLOB,
        p_dir                           IN      VARCHAR2,
        p_file                          IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Put_Remote_Ascii_Data (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_file                          IN      VARCHAR2,
        p_data                          IN      CLOB);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Put_Remote_Binary_Data (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_file                          IN      VARCHAR2,
        p_data                          IN      BLOB);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Get (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_from_file                     IN      VARCHAR2,
        p_to_dir                        IN      VARCHAR2,
        p_to_file                       IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Put (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_from_dir                      IN      VARCHAR2,
        p_from_file                     IN      VARCHAR2,
        p_to_file                       IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Get_Direct (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_from_file                     IN      VARCHAR2,
        p_to_dir                        IN      VARCHAR2,
        p_to_file                       IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Put_Direct (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_from_dir                      IN      VARCHAR2,
        p_from_file                     IN      VARCHAR2,
        p_to_file                       IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE HELP (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE ASCII (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE BINARY (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE LIST (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_dir                           IN      VARCHAR2,
        p_list                          OUT     t_string_table);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Nlst (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_dir                           IN      VARCHAR2,
        p_list                          OUT     t_string_table);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE RENAME (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_from                          IN      VARCHAR2,
        p_to                            IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE DELETE (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_file                          IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Mkdir (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_dir                           IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Rmdir (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_dir                           IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Convert_Crlf (
        p_status                        IN      BOOLEAN);
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE PACKAGE BODY jg_ftp AS
------------------------------------------------------------------------------------------------------------------------
    g_reply                         t_string_table := t_string_table ();
    g_binary                        BOOLEAN := TRUE;
    g_debug                         BOOLEAN := TRUE;
    g_convert_crlf                  BOOLEAN := TRUE;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE DEBUG (
        p_text                          IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    FUNCTION login (
        p_host                          IN      VARCHAR2,
        p_port                          IN      VARCHAR2,
        p_user                          IN      VARCHAR2,
        p_pass                          IN      VARCHAR2,
        p_timeout                       IN      NUMBER := NULL)
        RETURN UTL_TCP.connection IS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
    BEGIN
        g_reply.DELETE;
        l_conn  := UTL_TCP.open_connection (p_host, p_port, tx_timeout => p_timeout);
        get_reply (l_conn);
        send_command (l_conn, 'USER ' || p_user);
        send_command (l_conn, 'PASS ' || p_pass);
        RETURN l_conn;
    END;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_passive (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection)
        RETURN UTL_TCP.connection IS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
        l_reply                         VARCHAR2 (32767);
        --l_host    VARCHAR(100);
        l_port1                         NUMBER (10);
        l_port2                         NUMBER (10);
    BEGIN
        send_command (p_conn, 'PASV');
        l_reply  := g_reply (g_reply.LAST);
        l_reply  :=
            REPLACE (SUBSTR (l_reply, INSTR (l_reply, '(') + 1, (INSTR (l_reply, ')')) - (INSTR (l_reply, '(')) - 1),
                     ',',
                     '.');
        --l_host  := SUBSTR(l_reply, 1, INSTR(l_reply, '.', 1, 4)-1);
        l_port1  :=
            TO_NUMBER (SUBSTR (l_reply,
                               INSTR (l_reply, '.', 1, 4) + 1,
                               (INSTR (l_reply, '.', 1, 5) - 1) - (INSTR (l_reply, '.', 1, 4))));
        l_port2  := TO_NUMBER (SUBSTR (l_reply, INSTR (l_reply, '.', 1, 5) + 1));
        --l_conn := utl_tcp.open_connection(l_host, 256 * l_port1 + l_port2);
        l_conn   := UTL_TCP.open_connection (p_conn.remote_host, 256 * l_port1 + l_port2);
        RETURN l_conn;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE LOGOUT (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_reply                         IN      BOOLEAN := TRUE) AS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        send_command (p_conn, 'QUIT', p_reply);
        UTL_TCP.close_connection (p_conn);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_command (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_command                       IN      VARCHAR2,
        p_reply                         IN      BOOLEAN := TRUE) IS
------------------------------------------------------------------------------------------------------------------------
        l_result                        PLS_INTEGER;
    BEGIN
        l_result  := UTL_TCP.write_line (p_conn, p_command);

        -- If you get ORA-29260 after the PASV call, replace the above line with the following line.
        -- l_result := UTL_TCP.write_text(p_conn, p_command || utl_tcp.crlf, length(p_command || utl_tcp.crlf));
        IF p_reply
        THEN
            get_reply (p_conn);
        END IF;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get_reply (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection) IS
------------------------------------------------------------------------------------------------------------------------
        l_reply_code                    VARCHAR2 (3) := NULL;
    BEGIN
        LOOP
            g_reply.EXTEND;
            g_reply (g_reply.LAST)  := UTL_TCP.get_line (p_conn, TRUE);
            DEBUG (g_reply (g_reply.LAST));

            IF l_reply_code IS NULL
            THEN
                l_reply_code  := SUBSTR (g_reply (g_reply.LAST), 1, 3);
            END IF;

            IF SUBSTR (l_reply_code, 1, 1) IN ('4', '5')
            THEN
                Raise_Application_Error (-20000, g_reply (g_reply.LAST));
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
    FUNCTION get_local_ascii_data (
        p_dir                           IN      VARCHAR2,
        p_file                          IN      VARCHAR2)
        RETURN CLOB IS
------------------------------------------------------------------------------------------------------------------------
        l_bfile                         BFILE;
        l_data                          CLOB;
    BEGIN
        DBMS_LOB.createtemporary (lob_loc => l_data, CACHE => TRUE, dur => DBMS_LOB.CALL);
        l_bfile  := BFILENAME (p_dir, p_file);
        DBMS_LOB.fileopen (l_bfile, DBMS_LOB.file_readonly);

        IF DBMS_LOB.getlength (l_bfile) > 0
        THEN
            DBMS_LOB.loadfromfile (l_data, l_bfile, DBMS_LOB.getlength (l_bfile));
        END IF;

        DBMS_LOB.fileclose (l_bfile);
        RETURN l_data;
    END;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_local_binary_data (
        p_dir                           IN      VARCHAR2,
        p_file                          IN      VARCHAR2)
        RETURN BLOB IS
------------------------------------------------------------------------------------------------------------------------
        l_bfile                         BFILE;
        l_data                          BLOB;
    BEGIN
        DBMS_LOB.createtemporary (lob_loc => l_data, CACHE => TRUE, dur => DBMS_LOB.CALL);
        l_bfile  := BFILENAME (p_dir, p_file);
        DBMS_LOB.fileopen (l_bfile, DBMS_LOB.file_readonly);

        IF DBMS_LOB.getlength (l_bfile) > 0
        THEN
            DBMS_LOB.loadfromfile (l_data, l_bfile, DBMS_LOB.getlength (l_bfile));
        END IF;

        DBMS_LOB.fileclose (l_bfile);
        RETURN l_data;
    END;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_remote_ascii_data (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_file                          IN      VARCHAR2)
        RETURN CLOB IS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
        l_amount                        PLS_INTEGER;
        l_buffer                        VARCHAR2 (32767);
        l_data                          CLOB;
    BEGIN
        DBMS_LOB.createtemporary (lob_loc => l_data, CACHE => TRUE, dur => DBMS_LOB.CALL);
        l_conn  := get_passive (p_conn);
        send_command (p_conn, 'RETR ' || p_file, TRUE);

        BEGIN
            LOOP
                l_amount  := UTL_TCP.read_text (l_conn, l_buffer, 32767);
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
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_file                          IN      VARCHAR2)
        RETURN BLOB IS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
        l_amount                        PLS_INTEGER;
        l_buffer                        RAW (32767);
        l_data                          BLOB;
    BEGIN
        DBMS_LOB.createtemporary (lob_loc => l_data, CACHE => TRUE, dur => DBMS_LOB.CALL);
        l_conn  := get_passive (p_conn);
        send_command (p_conn, 'RETR ' || p_file, TRUE);

        BEGIN
            LOOP
                l_amount  := UTL_TCP.read_raw (l_conn, l_buffer, 32767);
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
    PROCEDURE put_local_ascii_data (
        p_data                          IN      CLOB,
        p_dir                           IN      VARCHAR2,
        p_file                          IN      VARCHAR2) IS
------------------------------------------------------------------------------------------------------------------------
        l_out_file                      UTL_FILE.file_type;
        l_buffer                        VARCHAR2 (32767);
        l_amount                        BINARY_INTEGER := 32767;
        l_pos                           INTEGER := 1;
        l_clob_len                      INTEGER;
    BEGIN
        l_clob_len  := DBMS_LOB.getlength (p_data);
        l_out_file  := UTL_FILE.fopen (p_dir, p_file, 'w', 32767);

        WHILE l_pos <= l_clob_len
        LOOP
            DBMS_LOB.READ (p_data, l_amount, l_pos, l_buffer);

            IF g_convert_crlf
            THEN
                l_buffer  := REPLACE (l_buffer, CHR (13), NULL);
            END IF;

            UTL_FILE.put (l_out_file, l_buffer);
            UTL_FILE.fflush (l_out_file);
            l_pos  := l_pos + l_amount;
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
    PROCEDURE put_local_binary_data (
        p_data                          IN      BLOB,
        p_dir                           IN      VARCHAR2,
        p_file                          IN      VARCHAR2) IS
------------------------------------------------------------------------------------------------------------------------
        l_out_file                      UTL_FILE.file_type;
        l_buffer                        RAW (32767);
        l_amount                        BINARY_INTEGER := 32767;
        l_pos                           INTEGER := 1;
        l_blob_len                      INTEGER;
    BEGIN
        l_blob_len  := DBMS_LOB.getlength (p_data);
        l_out_file  := UTL_FILE.fopen (p_dir, p_file, 'wb', 32767);

        WHILE l_pos <= l_blob_len
        LOOP
            DBMS_LOB.READ (p_data, l_amount, l_pos, l_buffer);
            UTL_FILE.put_raw (l_out_file, l_buffer, TRUE);
            UTL_FILE.fflush (l_out_file);
            l_pos  := l_pos + l_amount;
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
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_file                          IN      VARCHAR2,
        p_data                          IN      CLOB) IS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
        l_result                        PLS_INTEGER;
        l_buffer                        VARCHAR2 (32767);
        l_amount                        BINARY_INTEGER := 32767;
                                                -- Switch to 10000 (or use binary) if you get ORA-06502 from this line.
        l_pos                           INTEGER := 1;
        l_clob_len                      INTEGER;
    BEGIN
        l_conn      := get_passive (p_conn);
        
        send_command (p_conn, 'TYPE A', TRUE);
        send_command (p_conn, 'STOR ' || p_file, TRUE);
        l_clob_len  := DBMS_LOB.getlength (p_data);

        WHILE l_pos <= l_clob_len
        LOOP
            DBMS_LOB.READ (p_data, l_amount, l_pos, l_buffer);

            IF g_convert_crlf
            THEN
                l_buffer  := REPLACE (l_buffer, CHR (13), NULL);
            END IF;

            l_result  := UTL_TCP.write_text (l_conn, l_buffer, LENGTH (l_buffer));
            UTL_TCP.FLUSH (l_conn);
            l_pos     := l_pos + l_amount;
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
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_file                          IN      VARCHAR2,
        p_data                          IN      BLOB) IS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
        l_result                        PLS_INTEGER;
        l_buffer                        RAW (32767);
        l_amount                        BINARY_INTEGER := 32767;
        l_pos                           INTEGER := 1;
        l_blob_len                      INTEGER;
    BEGIN
        l_conn := get_passive (p_conn);
        
        --setting binary type 
        send_command (p_conn, 'TYPE I', TRUE);
        send_command (p_conn, 'STOR ' || p_file, TRUE);
        l_blob_len  := DBMS_LOB.getlength (p_data);

        WHILE l_pos <= l_blob_len
        LOOP
            DBMS_LOB.READ (p_data, l_amount, l_pos, l_buffer);
            l_result  := UTL_TCP.write_raw (l_conn, l_buffer, l_amount);
            UTL_TCP.FLUSH (l_conn);
            l_pos     := l_pos + l_amount;
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
    PROCEDURE get (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_from_file                     IN      VARCHAR2,
        p_to_dir                        IN      VARCHAR2,
        p_to_file                       IN      VARCHAR2) AS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        IF g_binary
        THEN
            put_local_binary_data (p_data     => get_remote_binary_data (p_conn, p_from_file),
                                   p_dir      => p_to_dir,
                                   p_file     => p_to_file);
        ELSE
            put_local_ascii_data (p_data     => get_remote_ascii_data (p_conn, p_from_file),
                                  p_dir      => p_to_dir,
                                  p_file     => p_to_file);
        END IF;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE put (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_from_dir                      IN      VARCHAR2,
        p_from_file                     IN      VARCHAR2,
        p_to_file                       IN      VARCHAR2) AS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        IF g_binary
        THEN
            put_remote_binary_data (p_conn     => p_conn,
                                    p_file     => p_to_file,
                                    p_data     => get_local_binary_data (p_from_dir, p_from_file));
        ELSE
            put_remote_ascii_data (p_conn     => p_conn,
                                   p_file     => p_to_file,
                                   p_data     => get_local_ascii_data (p_from_dir, p_from_file));
        END IF;

        get_reply (p_conn);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE get_direct (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_from_file                     IN      VARCHAR2,
        p_to_dir                        IN      VARCHAR2,
        p_to_file                       IN      VARCHAR2) IS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
        l_out_file                      UTL_FILE.file_type;
        l_amount                        PLS_INTEGER;
        l_buffer                        VARCHAR2 (32767);
        l_raw_buffer                    RAW (32767);
    BEGIN
        l_conn  := get_passive (p_conn);
        send_command (p_conn, 'RETR ' || p_from_file, TRUE);

        IF g_binary
        THEN
            l_out_file  := UTL_FILE.fopen (p_to_dir, p_to_file, 'wb', 32767);
        ELSE
            l_out_file  := UTL_FILE.fopen (p_to_dir, p_to_file, 'w', 32767);
        END IF;

        BEGIN
            LOOP
                IF g_binary
                THEN
                    l_amount  := UTL_TCP.read_raw (l_conn, l_raw_buffer, 32767);
                    UTL_FILE.put_raw (l_out_file, l_raw_buffer, TRUE);
                ELSE
                    l_amount  := UTL_TCP.read_text (l_conn, l_buffer, 32767);

                    IF g_convert_crlf
                    THEN
                        l_buffer  := REPLACE (l_buffer, CHR (13), NULL);
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
    PROCEDURE put_direct (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_from_dir                      IN      VARCHAR2,
        p_from_file                     IN      VARCHAR2,
        p_to_file                       IN      VARCHAR2) IS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
        l_bfile                         BFILE;
        l_result                        PLS_INTEGER;
        l_amount                        PLS_INTEGER := 32767;
        l_raw_buffer                    RAW (32767);
        l_len                           NUMBER;
        l_pos                           NUMBER := 1;
        ex_ascii                        EXCEPTION;
    BEGIN
        IF NOT g_binary
        THEN
            RAISE ex_ascii;
        END IF;

        l_conn   := get_passive (p_conn);
        send_command (p_conn, 'STOR ' || p_to_file, TRUE);
        l_bfile  := BFILENAME (p_from_dir, p_from_file);
        DBMS_LOB.fileopen (l_bfile, DBMS_LOB.file_readonly);
        l_len    := DBMS_LOB.getlength (l_bfile);

        WHILE l_pos <= l_len
        LOOP
            DBMS_LOB.READ (l_bfile, l_amount, l_pos, l_raw_buffer);
            DEBUG (l_amount);
            l_result  := UTL_TCP.write_raw (l_conn, l_raw_buffer, l_amount);
            l_pos     := l_pos + l_amount;
        END LOOP;

        DBMS_LOB.fileclose (l_bfile);
        UTL_TCP.close_connection (l_conn);
    EXCEPTION
        WHEN ex_ascii
        THEN
            Raise_Application_Error (-20000, 'PUT_DIRECT not available in ASCII mode.');
        WHEN OTHERS
        THEN
            IF DBMS_LOB.fileisopen (l_bfile) = 1
            THEN
                DBMS_LOB.fileclose (l_bfile);
            END IF;

            RAISE;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE HELP (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection) AS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        send_command (p_conn, 'HELP', TRUE);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE ASCII (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection) AS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        send_command (p_conn, 'TYPE A', TRUE);
        g_binary  := FALSE;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE BINARY (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection) AS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        send_command (p_conn, 'TYPE I', TRUE);
        g_binary  := TRUE;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE LIST (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_dir                           IN      VARCHAR2,
        p_list                          OUT     t_string_table) AS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
        l_list                          t_string_table := t_string_table ();
        l_reply_code                    VARCHAR2 (3) := NULL;
    BEGIN
        l_conn  := get_passive (p_conn);
        send_command (p_conn, 'LIST ' || p_dir, TRUE);

        BEGIN
            LOOP
                l_list.EXTEND;
                l_list (l_list.LAST)  := UTL_TCP.get_line (l_conn, TRUE);
                DEBUG (l_list (l_list.LAST));

                IF l_reply_code IS NULL
                THEN
                    l_reply_code  := SUBSTR (l_list (l_list.LAST), 1, 3);
                END IF;

                IF (    SUBSTR (l_reply_code, 1, 1) IN ('4', '5')
                    AND SUBSTR (l_reply_code, 4, 1) = ' ')
                THEN
                    Raise_Application_Error (-20000, l_list (l_list.LAST));
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

        l_list.DELETE (l_list.LAST);
        p_list  := l_list;
        UTL_TCP.close_connection (l_conn);
        get_reply (p_conn);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE nlst (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_dir                           IN      VARCHAR2,
        p_list                          OUT     t_string_table) AS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
        l_list                          t_string_table := t_string_table ();
        l_reply_code                    VARCHAR2 (3) := NULL;
    BEGIN
        l_conn  := get_passive (p_conn);
        send_command (p_conn, 'NLST ' || p_dir, TRUE);

        BEGIN
            LOOP
                l_list.EXTEND;
                l_list (l_list.LAST)  := UTL_TCP.get_line (l_conn, TRUE);
                DEBUG (l_list (l_list.LAST));

                IF l_reply_code IS NULL
                THEN
                    l_reply_code  := SUBSTR (l_list (l_list.LAST), 1, 3);
                END IF;

                IF (    SUBSTR (l_reply_code, 1, 1) IN ('4', '5')
                    AND SUBSTR (l_reply_code, 4, 1) = ' ')
                THEN
                    Raise_Application_Error (-20000, l_list (l_list.LAST));
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

        l_list.DELETE (l_list.LAST);
        p_list  := l_list;
        UTL_TCP.close_connection (l_conn);
        get_reply (p_conn);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE RENAME (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_from                          IN      VARCHAR2,
        p_to                            IN      VARCHAR2) AS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
    BEGIN
        l_conn  := get_passive (p_conn);
        send_command (p_conn, 'RNFR ' || p_from, TRUE);
        send_command (p_conn, 'RNTO ' || p_to, TRUE);
        LOGOUT (l_conn, FALSE);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE DELETE (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_file                          IN      VARCHAR2) AS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
    BEGIN
        l_conn  := get_passive (p_conn);
        send_command (p_conn, 'DELE ' || p_file, TRUE);
        LOGOUT (l_conn, FALSE);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE mkdir (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_dir                           IN      VARCHAR2) AS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
    BEGIN
        l_conn  := get_passive (p_conn);
        send_command (p_conn, 'MKD ' || p_dir, TRUE);
        LOGOUT (l_conn, FALSE);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE rmdir (
        p_conn                          IN OUT NOCOPY UTL_TCP.connection,
        p_dir                           IN      VARCHAR2) AS
------------------------------------------------------------------------------------------------------------------------
        l_conn                          UTL_TCP.connection;
    BEGIN
        l_conn  := get_passive (p_conn);
        send_command (p_conn, 'RMD ' || p_dir, TRUE);
        LOGOUT (l_conn, FALSE);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE convert_crlf (
        p_status                        IN      BOOLEAN) AS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        g_convert_crlf  := p_status;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE DEBUG (
        p_text                          IN      VARCHAR2) IS
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
CREATE OR REPLACE PACKAGE JG_FTP_CONFIGURATION IS
------------------------------------------------------------------------------------------------------------------------
  
    sf_ftp_host                  VARCHAR2(30) := '193.202.117.201';
    sf_ftp_user                  VARCHAR2(30) := 'jbs';
    sf_ftp_password              VARCHAR2(30) := 'p6ucuyUk';
    sf_ftp_port                  PLS_INTEGER  := 21;    
    
    sf_ftp_in_folder             VARCHAR2(30) := 'IN';
    sf_ftp_out_folder            VARCHAR2(30) := 'OUT';
    sf_ftp_out_archive_folder    VARCHAR2(30) := 'OUT/Archive';
    
------------------------------------------------------------------------------------------------------------------------
END;
/
CREATE OR REPLACE PACKAGE jg_input_sync IS
------------------------------------------------------------------------------------------------------------------------
    sf_order_wzrc_id                lg_documents_templates.id%TYPE := 1000692;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Process_All;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Get_From_Ftp;
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE PACKAGE BODY jg_input_sync
IS
    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_response
    IS
        v_ctx              DBMS_XMLSAVE.ctxtype;
        v_xml_clob         CLOB;
        v_xml_type         XMLTYPE;
        r_current_format   pa_xmltype.tr_format;
        v_sql_query        VARCHAR2 (4000);
        v_oryginal_id      VARCHAR2 (100);
    BEGIN
        r_current_format := pa_xmltype.biezacy_format;
        pa_xmltype.set_short_format_xml ();

        FOR r_oulo IN (SELECT id,
                              object_type,
                              xml,
                              file_name
                         FROM jg_input_log inlo
                        WHERE inlo.xml_response IS NULL)
        LOOP
            IF r_oulo.object_type = 'ORDER'
            THEN
                v_oryginal_id := NULL;

                BEGIN
                    v_oryginal_id :=
                        pa_xmltype.wartosc (px_xml      => xmltype (r_oulo.xml),
                                            p_sciezka   => '/Order/ID');
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        v_oryginal_id := 'TO_CHAR(NULL)';
                END;

                v_sql_query :=
                       'SELECT '
                    || v_oryginal_id
                    || ' ID,
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
                    || r_oulo.id;
                v_ctx := DBMS_XMLGEN.newcontext (querystring => v_sql_query);
                DBMS_XMLGEN.setrowtag (v_ctx, 'ORDER_RESPONSE');
                DBMS_XMLGEN.setrowsettag (v_ctx, NULL);
                v_xml_type := DBMS_XMLGEN.getxmltype (v_ctx);
                pa_xmltype.ustaw_format (r_current_format);
                DBMS_XMLGEN.closecontext (v_ctx);

                IF v_xml_type IS NOT NULL
                THEN
                    v_xml_clob := v_xml_type.getclobval ();

                    BEGIN
                        jg_output_sync.send_text_file_to_ftp (
                            p_xml         => v_xml_clob,
                            p_file_name   =>    '/IN/Response_'
                                             || r_oulo.file_name);

                        UPDATE jg_input_log
                           SET xml_response = v_xml_clob
                         WHERE id = r_oulo.id;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            NULL;
                    END;
                END IF;
            ELSIF r_oulo.object_type = 'NEW_CONTRACTORS'
            THEN
                v_oryginal_id := NULL;

                BEGIN
                    v_oryginal_id :=
                        pa_xmltype.wartosc (px_xml      => xmltype (r_oulo.xml),
                                            p_sciezka   => '/Order/ID');
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        v_oryginal_id := 'TO_CHAR(NULL)';
                END;

                v_sql_query :=
                       'SELECT '
                    || v_oryginal_id
                    || ' ID,
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
                    || r_oulo.id;
                v_ctx := DBMS_XMLGEN.newcontext (querystring => v_sql_query);
                DBMS_XMLGEN.setrowtag (v_ctx, 'ORDER_RESPONSE');
                DBMS_XMLGEN.setrowsettag (v_ctx, NULL);
                v_xml_type := DBMS_XMLGEN.getxmltype (v_ctx);
                pa_xmltype.ustaw_format (r_current_format);
                DBMS_XMLGEN.closecontext (v_ctx);

                IF v_xml_type IS NOT NULL
                THEN
                    v_xml_clob := v_xml_type.getclobval ();

                    BEGIN
                        jg_output_sync.send_text_file_to_ftp (
                            p_xml         => v_xml_clob,
                            p_file_name   =>    '/IN/Response_'
                                             || r_oulo.file_name);

                        UPDATE jg_input_log
                           SET xml_response = v_xml_clob
                         WHERE id = r_oulo.id;
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
    FUNCTION get_xslt_from_repository (
        p_object_type   IN jg_xslt_repository.object_type%TYPE)
        RETURN jg_xslt_repository.xslt%TYPE
    IS
        ------------------------------------------------------------------------------------------------------------------------
        CURSOR c_xslt (pc_object_type jg_xslt_repository.object_type%TYPE)
        IS
            SELECT xslt
              FROM jg_xslt_repository
             WHERE object_type = pc_object_type;

        v_xslt   jg_xslt_repository.xslt%TYPE;
    BEGIN
        OPEN c_xslt (p_object_type);

        FETCH c_xslt INTO v_xslt;

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

        FETCH c_sql_query INTO v_sql_query;

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
    PROCEDURE determine_object_type (
        p_xml            IN     CLOB,
        po_object_type      OUT jg_input_log.object_type%TYPE,
        po_on_time          OUT jg_input_log.on_time%TYPE)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        CURSOR c_main_node_name (pc_xml XMLTYPE)
        IS
            SELECT t_xml.COLUMN_VALUE.getrootelement () nodes
              FROM TABLE (XMLSEQUENCE (pc_xml)) t_xml;

        v_object_type   jg_input_log.object_type%TYPE;
    BEGIN
        po_object_type := NULL;

        OPEN c_main_node_name (xmltype (p_xml));

        FETCH c_main_node_name INTO v_object_type;

        CLOSE c_main_node_name;

        IF INSTR (UPPER (v_object_type), 'NEWCUSTOMER') > 0
        THEN
            po_object_type := 'NEW_CUSTOMER';
            po_on_time := 'T';
        ELSIF INSTR (UPPER (v_object_type), 'CUSTOMERDATA') > 0
        THEN
            po_object_type := 'CUSTOMER_DATA';
            po_on_time := 'T';
        ELSIF INSTR (UPPER (v_object_type), 'ORDER') > 0
        THEN
            po_object_type := 'ORDER';
            po_on_time := 'T';
        ELSE
            assert (
                FALSE,
                'Nie udało się określić typu obiektu na podstawie pliku');
        END IF;
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
        pa_xmltype.set_short_format_xml ();
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
        p_object_type   IN jg_xslt_repository.object_type%TYPE)
        RETURN XMLTYPE
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_xslt             jg_xslt_repository.xslt%TYPE;
        v_xml              XMLTYPE;
        r_current_format   pa_xmltype.tr_format;
        v_result           XMLTYPE;
    BEGIN
        r_current_format := pa_xmltype.biezacy_format;
        pa_xmltype.set_short_format_xml ();
        v_xslt := get_xslt_from_repository (p_object_type => p_object_type);
        v_xml := xmltype.createxml (p_xml);
        v_result := v_xml.transform (v_xslt);
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
        p_object_type   IN jg_xslt_repository.object_type%TYPE)
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
    FUNCTION import_sale_order (
        p_operation_id   IN jg_output_log.id%TYPE,
        p_object_type    IN jg_xslt_repository.object_type%TYPE)
        RETURN jg_input_log.object_id%TYPE
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_xml               XMLTYPE;
        v_xml_clob          CLOB;
        v_sql_query         CLOB;
        v_symbol            lg_sal_orders.symbol%TYPE;
        v_cinn_id           lg_sal_orders.cinn_id%TYPE;
        v_data_realizacji   lg_sal_orders.realization_date%TYPE;
        v_numer             NUMBER;
    BEGIN
        v_sql_query := get_query_from_sql_repository (p_object_type);
        v_sql_query :=
            REPLACE (v_sql_query, ':p_operation_id', p_operation_id);
        v_sql_query := REPLACE (v_sql_query, ':p_wzrc_id', sf_order_wzrc_id);
        v_xml_clob := create_xml (v_sql_query, p_object_type);
        v_xml :=
            transform_xml (p_xml => v_xml_clob, p_object_type => p_object_type);
        v_data_realizacji :=
            pa_xmltype.wartosc (v_xml, '/LG_ZASP_T/DATA_REALIZACJI');
        lg_dosp_numerowanie.ustal_kolejny_numer (
            po_symbol          => v_symbol,
            po_cinn_id         => v_cinn_id,
            po_numer           => v_numer,
            p_data_faktury     => v_data_realizacji,
            p_data_sprzedazy   => v_data_realizacji,
            p_wzrc_id          => sf_order_wzrc_id);
        v_xml :=
            xmltype.APPENDCHILDXML (
                v_xml,
                'LG_ZASP_T',
                xmltype (
                    '<SYMBOL_DOKUMENTU>' || v_symbol || '</SYMBOL_DOKUMENTU>'));
        apix_lg_zasp.aktualizuj (p_zamowienie => v_xml.getclobval);
        lg_dosp_obe.zakoncz;
        RETURN lg_sord_sql.id_symbol (
                   p_symbol   => pa_xmltype.wartosc (
                                    v_xml,
                                    '/LG_ZASP_T/SYMBOL_DOKUMENTU'));
    END;

    ------------------------------------------------------------------------------------------------------------------------
    PROCEDURE process (pr_operation IN jg_input_log%ROWTYPE)
    IS
        ------------------------------------------------------------------------------------------------------------------------
        v_object_id   jg_input_log.object_id%TYPE;
    BEGIN
        CASE pr_operation.object_type
            WHEN 'CUSTOMER_DATA'
            THEN
                v_object_id :=
                    import_customer (
                        p_xml           => pr_operation.xml,
                        p_object_type   => pr_operation.object_type);
            WHEN 'NEW_CUSTOMER'
            THEN
                v_object_id :=
                    import_customer (
                        p_xml           => pr_operation.xml,
                        p_object_type   => pr_operation.object_type);
            WHEN 'ORDER'
            THEN
                pa_wass_def.ustaw (p_nazwa     => 'IMPORT_INFINITE',
                                   p_wartosc   => 'T');
                v_object_id :=
                    import_sale_order (
                        p_operation_id   => pr_operation.id,
                        p_object_type    => pr_operation.object_type);
        END CASE;

        save_result (p_inlo_id     => pr_operation.id,
                     p_status      => 'PROCESSED',
                     p_object_id   => v_object_id);
        pa_wass_def.usun (p_nazwa => 'IMPORT_INFINITE');
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

                            IF INSTR (v_file_list (v_i), '.xml') > 0
                            THEN
                                v_file :=
                                    jg_ftp.get_remote_ascii_data (
                                        p_conn   => v_connection,
                                        p_file   =>    r_sqre.file_location
                                                    || '/'
                                                    || v_file_list (v_i));
                                determine_object_type (
                                    p_xml            => v_file,
                                    po_object_type   => v_object_type,
                                    po_on_time       => v_on_time);

                                INSERT INTO jg_input_log (id,
                                                          file_name,
                                                          object_type,
                                                          xml,
                                                          on_time)
                                     VALUES (jg_inlo_seq.NEXTVAL,
                                             v_file_list (v_i),
                                             v_object_type,
                                             v_file,
                                             v_on_time);

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

CREATE OR REPLACE PACKAGE jg_obop_def IS
------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Add_Operation (
        p_object_id                     IN      JG_observed_operations.object_id%TYPE,
        p_object_type                   IN      JG_observed_operations.object_type%TYPE,
        p_operation_type                IN      JG_observed_operations.operation_type%TYPE,
        p_attachment                    IN      jg_observed_operations.attachment%TYPE DEFAULT 'N');
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE PACKAGE BODY jg_obop_def IS
------------------------------------------------------------------------------------------------------------------------
    FUNCTION rt (
        p_object_id                     IN      jg_observed_operations.object_id%TYPE,
        p_object_type                   IN      jg_observed_operations.object_type%TYPE)
        RETURN jg_observed_operations%ROWTYPE IS
------------------------------------------------------------------------------------------------------------------------
        CURSOR c_operation (
            pc_object_id                            jg_observed_operations.object_id%TYPE,
            pc_object_type                          jg_observed_operations.object_type%TYPE) IS
            SELECT obop.*
              FROM jg_observed_operations obop
             WHERE     obop.object_id = pc_object_id
                   AND obop.object_type = pc_object_type;

        r_obop                          jg_observed_operations%ROWTYPE;
    BEGIN
        OPEN c_operation (p_object_id, p_object_type);

        FETCH c_operation
         INTO r_obop;

        CLOSE c_operation;

        RETURN r_obop;
    END;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION exist_operation (
        p_object_id                     IN      jg_observed_operations.object_id%TYPE,
        p_object_type                   IN      jg_observed_operations.object_type%TYPE)
        RETURN BOOLEAN IS
------------------------------------------------------------------------------------------------------------------------
        CURSOR c_operation (
            pc_object_id                            jg_observed_operations.object_id%TYPE,
            pc_object_type                          jg_observed_operations.object_type%TYPE) IS
            SELECT obop.id
              FROM jg_observed_operations obop
             WHERE     obop.object_id = pc_object_id
                   AND obop.object_type = pc_object_type;

        v_obop_id                       jg_observed_operations.id%TYPE;
    BEGIN
        OPEN c_operation (p_object_id, p_object_type);

        FETCH c_operation
         INTO v_obop_id;

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
        p_object_id                     IN      jg_observed_operations.object_id%TYPE,
        p_object_type                   IN      jg_observed_operations.object_type%TYPE,
        p_operation_type                IN      jg_observed_operations.operation_type%TYPE,
        p_attachment                    IN      jg_observed_operations.attachment%TYPE DEFAULT 'N') IS
------------------------------------------------------------------------------------------------------------------------
        r_obop                          jg_observed_operations%ROWTYPE;
    BEGIN
        IF p_object_id IS NOT NULL
        THEN
            r_obop  := rt (p_object_id, p_object_type);

            IF r_obop.id IS NULL
            THEN
                INSERT INTO jg_observed_operations
                            (id,
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

CREATE OR REPLACE PACKAGE jg_output_sync IS
------------------------------------------------------------------------------------------------------------------------
    TYPE tt_set_row IS RECORD (
        id                               NUMBER(10,0));

    TYPE tt_set_table IS TABLE OF tt_set_row;
    
------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Process;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Send_Text_File_To_Ftp (
        p_xml                           IN      CLOB,
        p_file_name                     IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Send_Binary_File_To_Ftp (
        p_byte                          IN      BLOB,
        p_file_name                     IN      VARCHAR2);

------------------------------------------------------------------------------------------------------------------------
    FUNCTION Format_Number (
        p_number                                NUMBER,
        p_digit                                 INT)
        RETURN VARCHAR2;
------------------------------------------------------------------------------------------------------------------------
END;
/

CREATE OR REPLACE PACKAGE BODY jg_output_sync IS
------------------------------------------------------------------------------------------------------------------------
    FUNCTION format_number (
        p_number                        IN      NUMBER,
        p_digit                         IN      INT)
        RETURN VARCHAR2 IS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        IF p_number IS NULL
        THEN
            RETURN NULL;
        ELSIF p_number = 0
        THEN
            RETURN 0;
        END IF;

        RETURN TRIM (TRAILING '.' FROM (TRIM (TRAILING 0 FROM TRIM (TO_CHAR (ROUND (p_number, p_digit), '9999999999999999999999999999990.000000000000000000')))));
    END;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION sqre_rt (
        p_object_type                   IN      jg_sql_repository.object_type%TYPE)
        RETURN jg_sql_repository%ROWTYPE IS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        FOR r_sqre IN (SELECT *
                         FROM jg_sql_repository sqre
                        WHERE     sqre.object_type = p_object_type
                              AND direction = 'OUT')
        LOOP
            RETURN r_sqre;
        END LOOP;

        RETURN NULL;
    END;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION get_query_from_sql_repository (
        p_object_type                   IN      jg_sql_repository.object_type%TYPE,
        po_xslt                         OUT     jg_sql_repository.xslt%TYPE,
        po_batch_guid                   OUT     jg_observed_operations.batch_guid%TYPE)
        RETURN jg_sql_repository.sql_query%TYPE IS
------------------------------------------------------------------------------------------------------------------------
        CURSOR c_sql_query (
            pc_object_type                          jg_sql_repository.object_type%TYPE) IS
            SELECT sql_query,
                   xslt
              FROM jg_sql_repository
             WHERE object_type = pc_object_type;

        v_sql_query                     jg_sql_repository.sql_query%TYPE;
        r_sqre                          jg_sql_repository%ROWTYPE;
    BEGIN
        r_sqre  := sqre_rt (p_object_type);

        IF r_sqre.sql_query IS NULL
        THEN
            assert (FALSE, 'Brak zdefiniowanego zapytania dla obiektu o typie ''' || p_object_type || '');
        ELSE
            po_xslt           := r_sqre.xslt;
            po_batch_guid     := SYS_GUID ();

            UPDATE jg_observed_operations
               SET batch_guid = po_batch_guid
             WHERE object_type = p_object_type
                   AND ROWNUM < 500;

            r_sqre.sql_query  :=
                REPLACE (r_sqre.sql_query,
                         ':p_id',
                         'SELECT object_id FROM jg_observed_operations WHERE batch_guid = ''' || po_batch_guid || '''');
        END IF;

        RETURN r_sqre.sql_query;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_result (
        p_object_type                   IN      jg_output_log.object_type%TYPE,
        p_batch_guid                    IN      jg_observed_operations.batch_guid%TYPE,
        p_xml                           IN      jg_output_log.xml%TYPE,
        p_status                        IN      jg_output_log.status%TYPE,
        p_file_name                     IN      jg_output_log.file_name%TYPE DEFAULT NULL,
        p_error                         IN      jg_output_log.error%TYPE DEFAULT NULL) IS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        INSERT INTO jg_output_log
                    (id,
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
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE save_result (
        p_guid                          IN      jg_output_log.guid%TYPE,
        p_status                        IN      jg_output_log.status%TYPE,
        p_file_name                     IN      jg_output_log.file_name%TYPE DEFAULT NULL,
        p_error                         IN      jg_output_log.error%TYPE DEFAULT NULL) IS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        UPDATE jg_output_log oulo
           SET oulo.status = p_status,
               oulo.file_name = p_file_name,
               oulo.error = p_error
         WHERE oulo.guid = p_guid;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_text_file_to_local_folder (
        p_xml                           IN      CLOB,
        p_file_name                     IN      VARCHAR2) IS
------------------------------------------------------------------------------------------------------------------------
        FILE                            UTL_FILE.file_type;
        l_pos                           INTEGER := 1;
        xml_len                         INTEGER;
        l_amount                        BINARY_INTEGER := 32767;
        l_buffer                        VARCHAR2 (32767);
    BEGIN
        FILE     := UTL_FILE.fopen (LOCATION => 'INFINITE', filename => p_file_name, open_mode => 'w');
        xml_len  := DBMS_LOB.getlength (p_xml);

        WHILE l_pos <= xml_len
        LOOP
            DBMS_LOB.READ (p_xml, l_amount, l_pos, l_buffer);
            l_buffer  := REPLACE (l_buffer, CHR (13), NULL);
            UTL_FILE.put (FILE => FILE, buffer => l_buffer);
            l_pos     := l_pos + l_amount;
        END LOOP;

        UTL_FILE.fclose (FILE => FILE);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE send_binary_file_to_loc_folder (
        p_byte                          IN      BLOB,
        p_file_name                     IN      VARCHAR2) IS
------------------------------------------------------------------------------------------------------------------------
        FILE                            UTL_FILE.file_type;
        l_pos                           INTEGER := 1;
        data_len                        INTEGER;
        l_amount                        BINARY_INTEGER := 32767;
        l_buffer                        RAW (32767);
    BEGIN
        FILE     := UTL_FILE.fopen (LOCATION => 'INFINITE', filename => p_file_name, open_mode => 'wb');
        data_len  := DBMS_LOB.getlength (p_byte);

        WHILE l_pos <= data_len
        LOOP
            DBMS_LOB.READ (p_byte, l_amount, l_pos, l_buffer);
            UTL_FILE.put_raw (FILE => FILE, buffer => l_buffer);
            UTL_FILE.fflush (FILE);
            l_pos     := l_pos + l_amount;
        END LOOP;

        UTL_FILE.fclose (FILE => FILE);

    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE generate_attachments (
        p_object_type                   IN      jg_observed_operations.object_type%TYPE,
        p_object_id                     IN      jg_observed_operations.object_id%TYPE,
        po_file_name                    OUT     jg_output_log.file_name%TYPE ) IS
------------------------------------------------------------------------------------------------------------------------
        CURSOR c_atta (
            pc_atta_id                  pa_attachments.id%TYPE ) IS
            SELECT atta.file_content file_content,
                   atta.filename file_name,
                   atus.guid object_guid
              FROM pa_attachments atta
              JOIN pa_attachment_uses atus
                ON atta.id = atus.atta_id
             WHERE atta.id = p_object_id;

        r_atta                          c_atta%ROWTYPE;
        v_object_id                     NUMBER(10,0);
        r_sqre                          jg_sql_repository%ROWTYPE;
        r_konr                          ap_kontrahenci%ROWTYPE;
        v_file_name                     jg_output_log.file_name%TYPE;
        v_file_extension                VARCHAR2(10);
        v_individual_contract           VARCHAR2(1);
    BEGIN
        IF p_object_type IN ('CONTRACT_ATTACHMENT')
        THEN
             OPEN c_atta(pc_atta_id => p_object_id);
            FETCH c_atta
             INTO r_atta;
            CLOSE c_atta;

            v_object_id := Lg_Konr_Sql.Id_Guid_Uk_Wr(p_guid => r_atta.object_guid);

            IF v_object_id IS NOT NULL
            THEN
                r_sqre  := sqre_rt ('TRADE_CONTRACTS');

                r_konr := Lg_Konr_Sql.Rt(p_id => v_object_id);

                IF r_konr.atrybut_t05 like '%UM IND%'
                THEN
                    v_file_name := r_sqre.file_location || '/' || r_konr.symbol || '_' || r_konr.nr_umowy_ind || '_' || r_konr.data_umowy_ind;
                ELSE
                    v_file_name := r_sqre.file_location || '/' || r_konr.symbol || '_' || r_konr.nr_umowy_ind || '_' || r_konr.atrybut_t05;
                END IF;

                v_file_extension := SUBSTR(r_atta.file_name, INSTR(r_atta.file_name, '.', -1));
                po_file_name := v_file_name || v_file_extension;

                Send_Binary_File_To_Ftp(p_byte => r_atta.file_content, p_file_name => po_file_name);
                --send_binary_file_to_loc_folder(p_byte => r_atta.file_content, p_file_name => po_file_name);
            END IF;
        END IF;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE delete_observed_operations (
        p_batch_guid                    IN      jg_observed_operations.batch_guid%TYPE) IS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        DELETE FROM jg_observed_operations
              WHERE batch_guid = p_batch_guid;
    END;

------------------------------------------------------------------------------------------------------------------------
    FUNCTION create_xml (
        p_sql_query                     IN      jg_sql_repository.sql_query%TYPE,
        p_xslt                          IN      jg_sql_repository.xslt%TYPE,
        p_object_type                   IN      jg_sql_repository.object_type%TYPE)
        RETURN CLOB IS
------------------------------------------------------------------------------------------------------------------------
        v_ctx                           DBMS_XMLSAVE.ctxtype;
        v_xml_clob                      CLOB;
        v_xml_type                      XMLTYPE;
        r_current_format                pa_xmltype.tr_format;
    BEGIN
        r_current_format  := pa_xmltype.biezacy_format;
        pa_xmltype.set_short_format_xml ();
        set_log('bb',p_sql_query);
        v_ctx             := DBMS_XMLGEN.newcontext (querystring => p_sql_query);
        DBMS_XMLGEN.setrowsettag (v_ctx, p_object_type);
        v_xml_type        := DBMS_XMLGEN.getxmltype (v_ctx);

        IF     p_xslt IS NOT NULL
           AND v_xml_type IS NOT NULL
        THEN
            v_xml_type  := v_xml_type.transform (XMLTYPE (p_xslt));
        END IF;

        pa_xmltype.ustaw_format (r_current_format);
        DBMS_XMLGEN.closecontext (v_ctx);

        IF v_xml_type IS NOT NULL
        THEN
            v_xml_clob  := v_xml_type.getclobval ();
        END IF;

        RETURN v_xml_clob;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Send_Text_File_To_Ftp (
        p_xml                           IN      CLOB,
        p_file_name                     IN      VARCHAR2) IS
------------------------------------------------------------------------------------------------------------------------
        v_connection                    UTL_TCP.connection;
    BEGIN
        v_connection  :=
            jg_ftp.login (p_host     => jg_ftp_configuration.sf_ftp_host,
                          p_port     => jg_ftp_configuration.sf_ftp_port,
                          p_user     => jg_ftp_configuration.sf_ftp_user,
                          p_pass     => jg_ftp_configuration.sf_ftp_password);
        jg_ftp.put_remote_ascii_data (p_conn => v_connection, p_file => p_file_name, p_data => p_xml);
        jg_ftp.get_reply (v_connection);
        jg_ftp.LOGOUT (v_connection);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Send_Binary_File_To_Ftp (
        p_byte                          IN      BLOB,
        p_file_name                     IN      VARCHAR2) IS
------------------------------------------------------------------------------------------------------------------------
        v_connection                    UTL_TCP.connection;
    BEGIN
        v_connection  :=
            jg_ftp.login (p_host     => jg_ftp_configuration.sf_ftp_host,
                          p_port     => jg_ftp_configuration.sf_ftp_port,
                          p_user     => jg_ftp_configuration.sf_ftp_user,
                          p_pass     => jg_ftp_configuration.sf_ftp_password);

        jg_ftp.Put_Remote_Binary_Data (p_conn => v_connection, p_file => p_file_name, p_data => p_byte);
        jg_ftp.get_reply (v_connection);
        jg_ftp.LOGOUT (v_connection);
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Process IS
------------------------------------------------------------------------------------------------------------------------
        v_sql_query                     jg_sql_repository.sql_query%TYPE;
        v_xml                           CLOB;
        v_file_name                     jg_output_log.file_name%TYPE;
        v_file_location                 jg_sql_repository.file_location%TYPE;
        v_batch_guid                    jg_observed_operations.batch_guid%TYPE;
        v_xslt                          jg_sql_repository.xslt%TYPE;
        r_sqre                          jg_sql_repository%ROWTYPE;
        v_status                        VARCHAR2 (10);
        c_oper                          SYS_REFCURSOR;
        v_count                         NUMBER;
    BEGIN
        LOOP
            OPEN c_oper FOR SELECT COUNT(id) FROM jg_observed_operations WHERE batch_guid IS NULL;
            FETCH c_oper
             INTO v_count;

            EXIT WHEN v_count = 0;

            FOR r_operation IN (SELECT object_type
                                  FROM jg_observed_operations
                                 WHERE attachment = 'N'
                                GROUP BY object_type)
            LOOP
                r_sqre  := sqre_rt (r_operation.object_type);
                SAVEPOINT create_xml;

                BEGIN
                    v_sql_query  := get_query_from_sql_repository (r_operation.object_type, v_xslt, v_batch_guid);
                    v_xml        := create_xml (v_sql_query, v_xslt, r_operation.object_type);

                    IF v_xml IS NULL
                    THEN
                        v_status  := 'NO_DATA';
                    ELSIF r_sqre.up_to_date = 'T'
                    THEN
                        v_status  := 'READY';
                    ELSE
                        v_status  := 'SKIPPED';
                    END IF;

                    save_result (p_object_type     => r_operation.object_type,
                                 p_batch_guid      => v_batch_guid,
                                 p_xml             => v_xml,
                                 p_status          => v_status);

                    delete_observed_operations (v_batch_guid);
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        ROLLBACK TO create_xml;
                        save_result (p_object_type     => r_operation.object_type,
                                     p_batch_guid      => v_batch_guid,
                                     p_xml             => v_xml,
                                     p_status          => 'ERROR',
                                     p_error           => SQLERRM || CHR (13) || DBMS_UTILITY.format_error_backtrace);
                END;
            END LOOP;
        END LOOP;

        FOR r_operation IN (SELECT *
                              FROM jg_output_log
                             WHERE status = 'READY')
        LOOP
            v_status := NULL;

            SAVEPOINT send_file;
            r_sqre  := sqre_rt (r_operation.object_type);

            BEGIN
                v_file_name  := NVL (r_sqre.file_location, 'IN/') || '/' || REPLACE (r_operation.object_type || '_' || r_operation.id || '_'
                                || TO_CHAR (SYSTIMESTAMP, 'YYYYMMDD_HH24MISS') || '.xml', '/', '-');
                Send_Text_File_To_Ftp (p_xml => r_operation.xml, p_file_name => v_file_name);
                save_result (p_guid => r_operation.guid, p_status => 'PROCESSED', p_file_name => v_file_name);
            EXCEPTION
                WHEN OTHERS
                THEN
                    ROLLBACK TO send_file;
                    save_result (p_guid       => r_operation.guid,
                                 p_status     => 'ERROR',
                                 p_error      => SQLERRM || CHR (13) || DBMS_UTILITY.format_error_backtrace);
            END;
        END LOOP;

        FOR r_operation IN (SELECT * FROM jg_observed_operations WHERE attachment = 'T')
        LOOP
            SAVEPOINT send_atta;
            BEGIN
                v_batch_guid := SYS_GUID;
                generate_attachments(p_object_type => r_operation.object_type, p_object_id => r_operation.object_id, po_file_name => v_file_name);

                save_result (p_object_type     => r_operation.object_type,
                             p_batch_guid      => v_batch_guid,
                             p_xml             => NULL,
                             p_file_name       => v_file_name,
                             p_status          => 'PROCESSED');

                DELETE FROM jg_observed_operations WHERE id = r_operation.id;
            EXCEPTION
                WHEN OTHERS
                THEN
                    ROLLBACK TO send_atta;
                    save_result (p_object_type  => r_operation.object_type,
                                 p_batch_guid   => v_batch_guid,
                                 p_xml          => NULL,
                                 p_status       => 'ERROR',
                                 p_error        => SQLERRM || CHR (13) || DBMS_UTILITY.format_error_backtrace);
            END;
        END LOOP;
    END;

------------------------------------------------------------------------------------------------------------------------
    PROCEDURE Retry (
        p_id                            IN      jg_output_log.id%TYPE) IS
------------------------------------------------------------------------------------------------------------------------
    BEGIN
        NULL;
    END;
------------------------------------------------------------------------------------------------------------------------
END;
/
CREATE OR REPLACE FUNCTION jg_dynamic_set_commponents (
    p_skkp_id    lg_kpl_skladniki_kompletu.id%TYPE)
    RETURN pa_lista_id.tt_lista_id PIPELINED AS
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
            FETCH c_inma INTO v_inma_id;

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
