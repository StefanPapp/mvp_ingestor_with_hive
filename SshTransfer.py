"""
 Transfers Files from Remote server to Edge node

 Author: Stefan Papp

"""

import paramiko
import logging


class SshTransfer(object):

    server = ""
    username = ""
    password = ""
    remote_base_dir = ""
    edge_base_dir = ""
    file_categories = []

    def set_connection(self, server, username, password, remote_path, local_path):
        self.server = server
        self.username = username
        self.password = password
        self.remote_base_dir = remote_path
        self.edge_base_dir = local_path

    def set_file_cat(self, fc):
        self.file_categories = fc

    def download_files(self):
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()

        logger = logging.getLogger("Ingest")
        logger.debug("INFO: Logging to SSH with Server, Username " +  self.server + " " + self.username)
        try:
            ssh.connect(self.server, username=self.username, password=self.password)
        except paramiko.BadHostKeyException:
            logger.error("the server’s host key could not be verified")
        except paramiko.AuthenticationException:
            logger.error("authentication failed")
        except paramiko.SSHException:
            logger.error("other SSH Exception")

        logger.info("SUCCESS: SSH connected established")

        logger.debug("INFO: Open SFTP")
        sftp = ssh.open_sftp()
        logger.info("SUCCESS: sftp opened. using directory + " + self.remote_base_dir)

        logger.debug("INFO: Downloading files to edge node")

        for fc in self.file_categories:
            source_directory = self.remote_base_dir + "/" + fc.get_sub_dir
            destination_directory = self.edge_base_dir + "/" + fc.get_sub_dir
            all_objects = sftp.listdir(source_directory)
            for current_file in all_objects:
                sftp.get(source_directory + '/' + current_file, destination_directory + '/'+ current_file)
                logger.info("SUCCESS: transferred file " + source_directory + '/' + current_file)
                if fc.get_archive_action == "keep":
                    try:
                        sftp.rename(source_directory + '/' + current_file, source_directory + '/archive/' + current_file)
                    except IOError:
                        logger.error("ERROR: Could not move file to archive " + source_directory + '/' + current_file)
                    logger.info("SUCCESS: file archived to " + source_directory + '/' + current_file, source_directory + '/archive/' + current_file)
                else:
                    try:
                        sftp.remove(source_directory + '/' + current_file)
                    except IOError:
                        logger.error("ERROR: Could not remove file " + source_directory + '/' + current_file)
                    logger.info("SUCCESS: file deleted")

        logger.debug("INFO: Closing sftp")
        sftp.close()
        logger.info("SUCCESS: sftp closed")

        logger.debug("INFO: Closing ssh")
        ssh.close()
        logger.info("SUCCESS: ssh closed")





