import os
import logging

class HadoopUpload(object):
    landing_directory = ''
    source_directory = ''

    def set_dir(self, sd, ld):
        self.landing_directory = ld
        self.source_directory = sd

    def upload_hadoop(self):
	logger = logging.getLogger("Ingest")
	logger.info("source directory: " + self.source_directory)
	all_objects = os.listdir(self.source_directory)
        for current_file in all_objects:
            os.system('hdfs dfs -put ' + current_file + ' ' + self.landing_directory)
	        os.system('pig -useHCatalog -p INPUTFILE=' + current_file + ' -p LANDDIR=' + self.landing_directory + ' pig_convert.pig')


