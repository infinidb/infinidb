create table extentmap (
	em_range_start bigint,
	em_range_size int,
	em_file_id int,
	em_block_offset int,
	em_hwm int,
	em_partition_num int,
	em_segment_num smallint,
	em_dbroot smallint,
	em_col_width smallint,
	em_status smallint,
	em_cp_max bigint,
	em_cp_min bigint,
	em_cp_seq int,
	em_cp_status tinyint
) engine=infinidb;

