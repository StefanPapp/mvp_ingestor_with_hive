import paramiko
import logging

class SshTransfer(object):

    server = ""
    username = ""
    password = ""
    remote_path = ""
    local_path = ""

    def set_connection(self, server, username, password, remote_path, local_path):
        self.server = server
        self.username = username
        self.password = password
        self.remote_path = remote_path
        self.local_path = local_path

    def download_all_files(self):
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()

        logger = logging.getLogger("Ingest")
        logger.info(self.server + " " + self.username + " " + self.password)
        ssh.connect(self.server, username=self.username, password=self.password)
        logger.info("ssh connected")

        sftp = ssh.open_sftp()
        logger.info("sftp opened. using + " + self.remote_path)
        all_objects = sftp.listdir(self.remote_path)

        logger.info("parsing all files")
        for afile in all_objects:
            print(afile)
            sftp.get(self.remote_path + '/' + afile, self.local_path + '/'+ afile)

        sftp.close()
        logger.info("sftp closed")
        ssh.close()





