from SshTransfer import SshTransfer
from HadoopUpload import HadoopUpload
import ConfigParser
import logging


class Ingest(object):
    config = ConfigParser.RawConfigParser()
    logger = logging.getLogger("Ingest")
    ssh_transfer = SshTransfer()
    hadoop_upload = HadoopUpload()

    def configure_ssh_transfer(self):
        self.config.read('./conf/config.cfg')
        server = self.config.get('ssh', 'server')
        username = self.config.get('ssh', 'username')
        password = self.config.get('ssh', 'password')
        remote_path = self.config.get('ssh', 'remote_path')
        local_path = self.config.get('edge', 'local_path')
        self.ssh_transfer.set_connection(server, username, password, remote_path, local_path)

    def configure_hadoop_upload(self):
        self.config.read('./conf/config.cfg')
        local_path =  self.config.get('edge', 'local_path')
        landing_dir = self.config.get('hadoop', 'landing_directory')
        self.hadoop_upload.set_dir(local_path, landing_dir)
        self.logger.info("Setting edge node dir and hdfs dir to " + local_path + " " + landing_dir)

    def configure_logger(self):
        self.logger.setLevel(logging.INFO)

        # log to console
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)

    def run(self):
        self.configure_logger()
        self.configure_ssh_transfer()
        self.configure_hadoop_upload()
        self.logger.info("Downloading files from " + self.ssh_transfer.server)
        self.ssh_transfer.download_all_files()

        #upload to hadooop
        self.hadoop_upload.upload_hadoop()

if __name__ == '__main__':
    Ingest().run()
