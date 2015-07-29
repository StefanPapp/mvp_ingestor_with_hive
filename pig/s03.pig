cd $LANDDIR
collection = LOAD '$INPUTFILE' USING PigStorage(';')
AS (
	meas_day: long
	imsi: long
	luacc_nu: int,
	luacc_pu: int,
	ho_from_3g_ne: int,
	tc_not_resp: int,
	calls_fail: int,
	calls: int,
	tracefile: chararray
);

STORE collection INTO 's03' USING org.apache.hcatalog.pig.HCatStorer();
