cd $LANDDIR
collection = LOAD '$INPUTFILE' USING PigStorage('|')
AS (
	datetime:chararray, 	
	msisdn:long,	
	max_downlink_kbps:double,	
	max_uplink_kbps:double,	
	med_downlink_kbps:double,	
	med_uplink_kbps:double,	
	med_dl_toppc_kbps:double,	
	med_ul_toppc_kbps:double,	
	active_average_dl_kbps:double,	
	active_average_ul_kbps:double,	
	num_5s_samples:long,	
	dl_burst:double,
	ul_burst:double,	
	creation_date:chararray);

STORE collection INTO 'procera' USING org.apache.hcatalog.pig.HCatStorer();
