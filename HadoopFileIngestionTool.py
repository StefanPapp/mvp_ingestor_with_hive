"""
 HadoopFileIngestionTool(object)
 - loads data from remote servers grouped by subdirectories
 - temporarily stores them on the Hadoop edge node
 - uploads them to Hadoop
 - registers them into HCatalog.

 Author: Stefan Papp

"""

import ConfigParser
import logging
import string

from FileCategory import FileCategory
from SshTransfer import SshTransfer
from HadoopUpload import HadoopUpload


class HadoopFileIngestionTool(object):
    config = ConfigParser.RawConfigParser()
    logger = logging.getLogger("HadoopFileIngestionTool")
    ssh_transfer = SshTransfer()
    hadoop_upload = HadoopUpload()
    file_categories = []

    def configure_logger(self):
        """ Log Info to console, Warn+Errors to a local log file "error.log"
        """
        self.logger.setLevel(logging.INFO)

        # log info to console
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        stream_handler.setFormatter(formatter)

        # log warnings and errors to file
        file_handler = logging.FileHandler('error.log')
        file_handler.setLevel(logging.WARN)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

    def configure_file_categories(self):
        """ Sets file categories. Each category is stored in a comma separated string.
            Each file category needs info such as subdir
        """
        self.config.read('./conf/config.cfg')
        file_categories = self.config.get('files', 'file_categories')

        file_type_array = string.split(file_categories, ",")
        for file_type in file_type_array:
            file_category = FileCategory()
            sub_dir = self.config.get(file_type,sub_dir)
            archive_action = self.config.get(file_type,archive_action)
            file_category.set_params(file_type, sub_dir, archive_action)
            file_categories.append(file_category)

    def configure_download(self):
        """ Sets basic parameters such as server ip, username for transfer
        """

        self.config.read('./conf/config.cfg')
        server = self.config.get('remote_server', 'server')
        username = self.config.get('remote_server', 'username')
        password = self.config.get('remote_server', 'password')
        remote_base_dir = self.config.get('remote_server', 'remote_base_dir')

        edge_base_dir = self.config.get('edge_node', 'edge_base_dir')
        self.ssh_transfer.set_connection(server, username, password, remote_base_dir, edge_base_dir)
        self.ssh_transfer.set_file_cat(self.file_categories)

    def configure_hadoop_upload(self):
        """ Sets basic parameters for hadoop upload
        """
        self.config.read('./conf/config.cfg')
        edge_base_dir = self.config.get('edge_node', 'edge_base_dir')
        landing_base_dir = self.config.get('hdfs', 'landing_base_dir')
        self.hadoop_upload.set_dir(edge_base_dir, landing_base_dir)

        self.hadoop_upload.set_file_cat(self.file_categories)
        self.logger.info("set edge node dir and hdfs dir to " + edge_base_dir + " " + landing_base_dir)

    def run(self):
        self.configure_logger()
        self.configure_file_categories()
        self.configure_download()
        self.configure_hadoop_upload()

        # download the files to the edge server
        self.ssh_transfer.download_files()

        # upload from edge server to hadooop
        self.hadoop_upload.upload_hadoop()

if __name__ == '__main__':
    HadoopFileIngestionTool().run()
