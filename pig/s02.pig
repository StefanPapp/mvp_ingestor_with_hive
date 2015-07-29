cd $LANDDIR
collection = LOAD '$INPUTFILE' USING PigStorage(';')
AS (
	day:long,
	interface:chararray,
	locupdates:int,
	imsi:long
);

STORE collection INTO 's02' USING org.apache.hcatalog.pig.HCatStorer();
