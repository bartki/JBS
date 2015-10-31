CREATE TABLE JG_SQL_REPOSITORY
(
  ID                NUMBER(10,0),
  OBJECT_TYPE       VARCHAR2(30)    NOT NULL,
  SQL_QUERY         CLOB,
  XSLT			        CLOB,
  FILE_LOCATION     VARCHAR2(250),
  UP_TO_DATE        VARCHAR2(1 BYTE) DEFAULT 'T' NOT NULL,
  DIRECTION         VARCHAR2(3)
)
/
ALTER TABLE JG_SQL_REPOSITORY
ADD CONSTRAINT JG_SQRE_PK PRIMARY KEY (ID)
/
ALTER TABLE JG_SQL_REPOSITORY
ADD CONSTRAINT JG_SQRE_OBJECT_TYPE UNIQUE (OBJECT_TYPE)
/

CREATE SEQUENCE JG_SQRE_SEQ
    MINVALUE 1
    MAXVALUE 9999999999999999999999999999
    START WITH 1
    INCREMENT BY 1
    CACHE 20
/

CREATE TABLE JG_OBSERVED_OPERATIONS
(
  ID                NUMBER(10,0),
  OBJECT_TYPE       VARCHAR2(30),
  OBJECT_ID         NUMBER(10,0),
  OPERATION_TYPE    VARCHAR2(6),
  BATCH_GUID        VARCHAR2(40)
)
/
ALTER TABLE JG_OBSERVED_OPERATIONS
ADD CONSTRAINT JG_OBOP_PK PRIMARY KEY (ID)
/
ALTER TABLE JG_OBSERVED_OPERATIONS
ADD CONSTRAINT JG_OBOP_OPERATION_TYPE CHECK (OPERATION_TYPE IN ('INSERT', 'UPDATE', 'DELETE', 'SKIPPED', 'NO_DATA'))
/
ALTER TABLE JG_OBSERVED_OPERATIONS
ADD CONSTRAINT JG_OBOP_OBJECT_ID_TYPE UNIQUE (OBJECT_TYPE, OBJECT_ID)
/
CREATE SEQUENCE JG_OBOP_SEQ
    MINVALUE 1
    MAXVALUE 9999999999999999999999999999
    START WITH 1
    INCREMENT BY 1
    CACHE 20
/
CREATE TABLE JG_OUTPUT_LOG
(
  ID                NUMBER(10,0),
  GUID              VARCHAR2(32) DEFAULT SYS_GUID(),
  LOG_DATE          DATE DEFAULT SYSDATE,
  OBJECT_TYPE       VARCHAR2(30)  NOT NULL,
  STATUS            VARCHAR2(9)  DEFAULT 'READY',
  XML               CLOB,
  ERROR             CLOB,
  FILE_NAME      VARCHAR2(100)
)
/

ALTER TABLE JG_OUTPUT_LOG
ADD CONSTRAINT JG_OULO_PK PRIMARY KEY (ID)
/
ALTER TABLE JG_OUTPUT_LOG
ADD CONSTRAINT JG_OULO_STATUS CHECK (STATUS IN ('READY', 'PROCESSED', 'ERROR', 'SKIPPED', 'NO_DATA'))
/
CREATE SEQUENCE JG_OULO_SEQ
    MINVALUE 1
    MAXVALUE 9999999999999999999999999999
    START WITH 1
    INCREMENT BY 1
    CACHE 20
/

BEGIN
    Api_Pa_Obie.Register_Table(p_object_name => 'JG_SQL_REPOSITORY', p_subsystem_code => 'PA', p_alias => 'SQRE');
    Api_Pa_Obie.Register_Table(p_object_name => 'JG_OBSERVED_OPERATIONS', p_subsystem_code => 'PA', p_alias => 'JOBOP');
    Api_Pa_Obie.Register_Table(p_object_name => 'JG_OUTPUT_LOG', p_subsystem_code => 'PA', p_alias => 'OULO');
    Api_Pa_Obie.Register_Sequence(p_object_name => 'JG_OBOP_SEQ', p_subsystem_code => 'PA');
    Api_Pa_Obie.Register_Sequence(p_object_name => 'JG_OULO_SEQ', p_subsystem_code => 'PA');
    Api_Pa_Obie.Register_Sequence(p_object_name => 'JG_SQRE_SEQ', p_subsystem_code => 'PA');    
    Api_Pa_Obie.Register_Package (p_object_name => 'JG_FTP', p_subsystem_code => 'PA');
    Api_Pa_Obie.Register_Package (p_object_name => 'JG_FTP_CONFIGURATION', p_subsystem_code => 'PA');
    Api_Pa_Obie.Register_Package (p_object_name => 'JG_INPUT_SYNC', p_subsystem_code => 'PA');
    Api_Pa_Obie.Register_Package (p_object_name => 'JG_OUTPUT_SYNC', p_subsystem_code => 'PA');
    Api_Pa_Obie.Register_Package (p_object_name => 'JG_OBOP_DEF', p_subsystem_code => 'PA');
END;
/
