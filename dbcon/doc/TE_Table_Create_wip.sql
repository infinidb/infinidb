conn sys/qalpont!@<SID> as sysdba

create user TE identified by TE;
create user S_TE identified by TE;
create user TE_stage identified by TE;

grant connect, resource, select any table to S_TE;
grant connect, resource, select any table to TE;
grant connect, resource, select any table to TE_stage;

conn calpont/calpont@<SID>
EXECUTE pkg_calpont.cal_register_object_owner('TE', 'TE', TRUE);


conn s_te/te@<SID>

CREATE TABLE D_Adfamily (
    Adfam_Nbr   NUMBER,
    Adfam_Nm    VARCHAR2(256)   NOT NULL,
    Dflt_Ind    CHAR(1)     NOT NULL ,
    Measured    CHAR(1)     NOT NULL ,
    Processtype CHAR(1)     NOT NULL 
--  ,CONSTRAINT PK_D_ADFAMILY PRIMARY KEY (Adfam_Nbr)
    );


CREATE TABLE D_Campaign (
    Cmpgn_Nbr   NUMBER NOT NULL,
    Cmpgn_Nm    VARCHAR2(256) NOT NULL,
    Prod_Nbr    NUMBER NOT NULL,
    Prod_Nm     VARCHAR2(256) NOT NULL,
    Adv_Nbr     NUMBER NOT NULL,
    Adv_Nm  VARCHAR2(256) NOT NULL,
    Cust_Nbr    NUMBER NOT NULL,
    Cust_Nm     VARCHAR2(256) NOT NULL,
    Org_Nbr     NUMBER NOT NULL,
    Org_Nm  VARCHAR2(256) NOT NULL,
    TYPE        CHAR(1) NOT NULL ,
    Start_Date  DATE,
    End_Date    DATE,
    Status  CHAR(1) NOT NULL ,
    Keyword     CHAR(1) NOT NULL ,
    Processreach NUMBER NOT NULL,
    Fixed_Cost  NUMBER
--  ,CONSTRAINT PK_D_CAMPAIGN PRIMARY KEY (Cmpgn_Nbr)
    );


CREATE TABLE D_Creative (
    CreativeID  NUMBER NOT NULL,
    PathName    VARCHAR2(110) NOT NULL,
    Crtv_Alias_Nbr  VARCHAR2(70) NOT NULL
--,CONSTRAINT PK_D_Creative PRIMARY KEY (CreativeID)
);


CREATE TABLE D_Event (
    Evnt_Nbr    NUMBER,
    Site_Nbr    NUMBER NOT NULL,
    Evnt_Grp_Nbr NUMBER NOT NULL,
    Pg_Desc VARCHAR2(256) NOT NULL,
    Evnt_Grp_Nm VARCHAR2(256) NOT NULL
--,CONSTRAINT PK_D_Event    PRIMARY KEY (Evnt_Nbr)
);


CREATE TABLE D_Hour (
    Hr_Nbr  NUMBER NOT NULL,
    Hr_Nm       VARCHAR2(256) NOT NULL,
    Hr_Dt       TIMESTAMP NOT NULL,
    Hod_Nbr     NUMBER NOT NULL,
    Day_Nbr     NUMBER NOT NULL,
    Day_Nm  VARCHAR2(256) NOT NULL,
    Dow_Nbr     NUMBER NOT NULL,
    Dow_Nm  VARCHAR2(256) NOT NULL,
    Mnth_Nm     VARCHAR2(256) NOT NULL,
    Mnth_Nbr    NUMBER NOT NULL,
    Qtr_Nbr     NUMBER NOT NULL,
    Qtr_Nm  VARCHAR2(256) NOT NULL,
    Wk_Nbr  NUMBER NOT NULL,
    Wk_Nm       VARCHAR2(256) NOT NULL,
    Yr_Nbr  NUMBER NOT NULL,
    Hod_Nm  VARCHAR2(256) NOT NULL
--,CONSTRAINT PK_D_HOUR PRIMARY KEY (Hr_Nbr)
);


CREATE TABLE D_Package (
    Pkg_Nbr NUMBER,
    Pkg_Nm  VARCHAR2(256) NOT NULL
--,CONSTRAINT PK_D_Package  PRIMARY KEY (Pkg_Nbr)
);


CREATE TABLE D_Product_Buy (
    Product_Buy_Nbr NUMBER NOT NULL,
    Product_Buy_Nm  VARCHAR2(24) NOT NULL,
    Mediaredirect   NUMBER NOT NULL,
    Cookies     NUMBER NOT NULL,
    Product_Buy_Typ CHAR(1),
    Behavior    CHAR(1),
    Redirect    VARCHAR2(256),
    Price       NUMBER(10,4),
    Plan_Vol    NUMBER,
    Units       CHAR(3) NOT NULL
--,CONSTRAINT PK_D_PRODUCT_BUY  PRIMARY KEY (Product_Buy_Nbr)
);


CREATE TABLE D_Site (
    Site_Nbr    NUMBER NOT NULL,
    Site_Nm     VARCHAR2(256) NOT NULL,
    Org_Nbr     NUMBER NOT NULL,
    Publisher_Id    NUMBER NOT NULL,
    Publisher_Name  VARCHAR2(256) NOT NULL
--,CONSTRAINT PK_D_SITE PRIMARY KEY (Site_Nbr)
);


CREATE TABLE D_Campaign_SiteMeasure (
    Cmpgn_Nbr   NUMBER NOT NULL,
    SiteMeasure_Nbr     number NOT NULL
--,CONSTRAINT PK_D_CAMPAIGN_SITE_MSR    PRIMARY KEY (Cmpgn_Nbr, SiteMeasure_Nbr )
);

CREATE TABLE Customer_spacedesc (
    Cust_Nbr    NUMBER NOT NULL,
    SPACEDESC   VARCHAR2(256) NOT NULL,
    CMPGN_NBR   NUMBER,
    ENDDATE     NUMBER,
    ADNET_NBR   NUMBER,
    FIRST_FLAG  CHAR(1));



CREATE TABLE Tag (
    Tag_Key NUMBER NOT NULL,
    Tag_Name    Varchar2(256) NOT NULL,
    Tag_Value   Varchar2(256) NOT NULL
--,CONSTRAINT PK_TAG    PRIMARY KEY (Tag_Key)
);


CREATE TABLE Tag_Trans (
    Tag_Key NUMBER NOT NULL,
    Log_Key NUMBER NOT NULL
--,CONSTRAINT PK_TAG_TRANS  PRIMARY KEY (Tag_Key)
);

CREATE TABLE D_DayParts (
    Type                           VARCHAR2(3),
    Type_Desc                      VARCHAR2(50),
    HOD_Nbr                        NUMBER,
    DOW_Nbr                        NUMBER
--,CONSTRAINT PK_D_DayParts PRIMARY KEY (Type, Type_Desc, HOD_Nbr, DOW_Nbr)
);

CREATE TABLE D_GeoTarget (
    Oct1        VARCHAR2(80),
    State       CHAR(2),
    Msa         VARCHAR2(17)
--,CONSTRAINT PK_D_GeoTarget    PRIMARY KEY (Oct1, State, Msa)
);
       
CREATE TABLE Tag_Names (
      Tag_Id            NUMBER(8,0),
      Tag_Name          VARCHAR2(40) CONSTRAINT NN_Tag_Names_Name NOT NULL,
      Tag_Value         VARCHAR2(40) CONSTRAINT NN_Tag_Values_Value NOT NULL
--, CONSTRAINT PK_Tag_Names PRIMARY KEY (Tag_Id)
); 

CREATE TABLE Tag_Values (
      Log_Key           NUMBER,
      Tag_Id            NUMBER(8,0)
--,CONSTRAINT PK_Tag_Values PRIMARY KEY (Log_Key, Tag_Id)
);  
    

drop table f_trans;

CREATE TABLE f_Trans (
    Log_Key     Number,
    Prim_Cookie     Number ,
    Prim_Cookie_Flag    Char(1),
    Trans_Typ       Char(1),
    Remote_IP       Varchar2(15),
    dest_url        varchar2(4000),
    Trans_Time      Number(9,6) ,
    Content_Length  Number(4),
    Trans_Timestamp_source  timestamp,
    Http_Status     Number(3),
    Campaign_nbr_source Number(7),
    Site_nbr        Number(7),
    Creative_Width  Number(4),
    Creative_Height Number(4),
    Site_Section_ID Number(7),
    Creative_Group_ID   Number(7),
    overrides       Varchar2(4000),
    Server_Diagnostic   Char(1) ,
    Creative_Path       varchar2(400), 
    Ad_Family_ID_source number(7),
    Prim_Cookie_Domain  number(9),
    Server_ID           Number(3),
    Request_Subtype     Char(2),
    CreativeID      number(9) ,
    Product_Buy_ID  Number(7),
    TimeStamp_Correction    timestamp,
    Kwd_Details     Varchar2(10),
    Target_Details  Varchar2(20),
    Speed_Select    Varchar2(10),
    SSL_Enabled     Varchar2(3),
    Digital_Signature   Varchar2(40),
    Grp_Value       Varchar2(400),
    Evnt_Value      Varchar2(400),
    Evnt_ID     Number(7),
    Cmpgn_nbr       Number(7),
    record_Timestamp    timestamp,
    adfam_nbr       number,
    Revenue     number,
    pkg_nbr     Varchar(200),
    Fraud_Flag      char(1),
    Ping_type       char(1),
    Ping_type_Log_Key   Number
    ) ;


conn te_Stage/te@<SID>

drop sequence te_stage.seq_f_trans;
create sequence te_stage.seq_f_trans start with 1 cache 100;
grant select on te_stage.seq_f_trans to public;


