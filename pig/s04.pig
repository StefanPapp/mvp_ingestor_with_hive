cd $LANDDIR
collection = LOAD '$INPUTFILE' USING PigStorage(';')
AS (
	meas_day: long,
	imsi: long,
	cs_calls: int,
	cs_fail: int,
	ps_normal_end: int,
	regda: int,
	ps_abnormal_release: int,
	luacc_ia: int,
	luacc_nu: int,
	luacc_pu: int,
	lufail_nu: int,
	lurej_nu: int,
	lurej_pu: int,
	succ_reloc_voice: int
);

STORE collection INTO 's04' USING org.apache.hcatalog.pig.HCatStorer();
