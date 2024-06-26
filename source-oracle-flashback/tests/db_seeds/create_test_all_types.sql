CREATE TABLE flow_test.test_all_types(
	id integer PRIMARY KEY,
	nvchar2 NVARCHAR2(2000),
	vchar2 VARCHAR2(2000),
	single_nchar NCHAR,
	vchar VARCHAR(2000),
	num NUMBER (38, 9),
	small_int SMALLINT,
  integ INTEGER,
	double_precision DOUBLE PRECISION,
	float_126 FLOAT(126),
	real_num REAL,
	datetime DATE,
	ts TIMESTAMP,
	ts_nine TIMESTAMP(9),
	ts_tz TIMESTAMP WITH TIME ZONE,
	ts_tz_nine TIMESTAMP(9) WITH TIME ZONE,
	ts_local_tz TIMESTAMP WITH LOCAL TIME ZONE,
	ts_local_tz_nine TIMESTAMP(9) WITH LOCAL TIME ZONE,
	interval_year INTERVAL YEAR(4) TO MONTH,
	interval_day INTERVAL DAY TO SECOND
)
