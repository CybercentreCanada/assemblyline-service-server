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

class HelperNamespace(Namespace):
    def __init__(self, namespace=None):
        super().__init__(namespace=namespace)

    def on_download_file(self, sha256, dest_path):
        client_id = get_request_id(request)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Sending file to client, SHA256: {sha256}")

        temp_dir = os.path.join(tempfile.gettempdir(), sha256)
        try:
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)
            file_path = os.path.join(temp_dir, sha256)
            filestore.download(sha256, file_path)
            with open(file_path, 'rb') as f:
                return f.read(), dest_path
            # return filestore.get(sha256), dest_path
        finally:
            if temp_dir:
                shutil.rmtree(temp_dir)

    def on_get_classification_definition(self, yml_file):
        client_id = get_request_id(request)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Sending classification definition to client")

        classification_definition = forge.get_classification().__dict__['original_definition']
        return classification_definition, yml_file

    def on_get_system_constants(self, json_file):
        constants = forge.get_constants()
        client_id = get_request_id(request)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Sending system constants to client")

        out = {'FILE_SUMMARY': constants.FILE_SUMMARY,
               'RECOGNIZED_TAGS': constants.RECOGNIZED_TAGS,
               'RULE_PATH': constants.RULE_PATH,
               'STANDARD_TAG_CONTEXTS': constants.STANDARD_TAG_CONTEXTS,
               'STANDARD_TAG_TYPES': constants.STANDARD_TAG_TYPES
               }

        return out, json_file

    def on_start_download(self, sha256, file_path):
        client_id = get_request_id(request)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Sending file to client, SHA256: {sha256}")

        temp_dir = os.path.join(tempfile.gettempdir(), sha256)
        try:
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)
            temp_file_path = os.path.join(temp_dir, sha256)
            filestore.download(sha256, temp_file_path)

            offset = 0
            chunk_size = 64*1024
            with open(temp_file_path, 'rb') as f:
                for chunk in read_in_chunks(f, chunk_size):
                    self.socketio.emit('write_file_chunk', (file_path, offset, chunk), namespace=self.namespace, room=client_id)
                    offset += chunk_size


            # with open(file_path, 'rb') as f:
            #     return f.read(), dest_path
            # return filestore.get(sha256), dest_path
        finally:
            if temp_dir:
                shutil.rmtree(temp_dir)


    def on_upload_file(self, data, classification, service_name, sha256, ttl):
        client_id = get_request_id(request)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Received file from client, SHA256: {sha256}")

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
            if not filestore.exists(file_info['sha256']):
                filestore.upload(file_path, file_info['sha256'])
        finally:
            if temp_dir:
                shutil.rmtree(temp_dir)


def get_request_id(request_p):
    if hasattr(request_p, "sid"):
        return request_p.sid
    return None


def read_in_chunks(file_object, chunk_size):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data
