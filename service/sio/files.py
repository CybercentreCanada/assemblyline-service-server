import os
import shutil
import tempfile

from flask import request
from flask_socketio import Namespace

from assemblyline.common import forge
from assemblyline.common import identify
from assemblyline.common.isotime import now_as_iso
from service.config import LOGGER

filestore = forge.get_filestore()
datastore = forge.get_datastore()


class FilesNamespace(Namespace):
    def __init__(self, namespace=None):
        super().__init__(namespace=namespace)

    def on_download_file(self, sha256, file_path):
        client_id = get_request_id(request)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Sending file to client, SHA256: {sha256}")
        return filestore.get(sha256), file_path

    def on_upload_file(self, data, classification, service_name, sha256, ttl):
        client_id = get_request_id(request)
        temp_dir = os.path.join(tempfile.gettempdir(), service_name)
        try:
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)
            file_path = os.path.join(temp_dir, sha256)
            with open(file_path, 'wb') as f:
                f.write(data)
                f.close()

            file_info = identify.fileinfo(file_path)
            file_info['classification'] = classification
            file_info['expiry_ts'] = now_as_iso(ttl * 24 * 60 * 60)
            datastore.save_or_freshen_file(file_info['sha256'], file_info, file_info['expiry_ts'], file_info['classification'])
            LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Received file from client, SHA256: {sha256}")
        finally:
            if temp_dir:
                shutil.rmtree(temp_dir)


def get_request_id(request_p):
    if hasattr(request_p, "sid"):
        return request_p.sid
    return None
