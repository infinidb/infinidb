-- Additional check constraint from QA
CREATE TABLE calpont.PART( p_partkey int not null, p_a varchar(55), p_b
decimal(8,2), p_c int default 1, p_d varchar(25) constraint checkitout check
(varchar = 'a' ))
