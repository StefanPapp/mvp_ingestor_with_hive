"""
 Upload from Edge to HDFS

 Author: Stefan Papp

"""

import os
import subprocess
import logging


class HadoopUpload(object):
    hdfs_base_dir = ""
    edge_base_dir = ""
    file_categories = []

    def set_dir(self, sd, ld):
        self.hdfs_base_dir = ld
        self.edge_base_dir = sd

    def set_file_cat(self, fc):
        self.file_categories = fc

    def upload_hadoop(self):
        logger = logging.getLogger("Ingest")
        logger.info("setting base source directory: " + self.edge_base_dir)

        for fc in self.file_categories:
            source_directory = self.edge_base_dir + "/" + fc.get_sub_dir
            destination_dir = self.hdfs_base_dir + "/" + fc.get_sub_dir
            all_objects = os.listdir(source_directory)
            for current_file in all_objects:
                try:
                    subprocess.call('hdfs dfs -put ' + current_file + ' ' + destination_dir, shell= True)
                except OSError as e:
                    logger.error("Could not upload file to hadoop " + current_file + ' ' + destination_dir)

                # os.system('pig -useHCatalog -p INPUTFILE=' + current_file + ' -p LANDDIR=' + destination_dir + ' ' + fc.get_file_type + '.pig')
                subprocess.call('pig -useHCatalog -p INPUTFILE=' + current_file + ' -p LANDDIR=' + destination_dir + ' ' + fc.get_file_type + '.pig', shell= True)

