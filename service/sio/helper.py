import os
import shutil
import tempfile

from assemblyline.common import forge
from assemblyline.common import identify
from assemblyline.common.isotime import now_as_iso
from assemblyline.odm.models.service import Service
from service.sio.base import BaseNamespace, authenticated_only, LOGGER

filestore = forge.get_filestore()
datastore = forge.get_datastore()

class HelperNamespace(BaseNamespace):
    @authenticated_only
    def on_get_classification_definition(self, yml_file, client_info):
        LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                    f"Sending classification definition to client")

        classification_definition = forge.get_classification().__dict__['original_definition']
        return classification_definition, yml_file

    @authenticated_only
    def on_get_system_constants(self, json_file, client_info):
        constants = forge.get_constants()
        LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                    f"Sending system constants to client")

        out = {'FILE_SUMMARY': constants.FILE_SUMMARY,
               'RECOGNIZED_TAGS': constants.RECOGNIZED_TAGS,
               'RULE_PATH': constants.RULE_PATH,
               'STANDARD_TAG_CONTEXTS': constants.STANDARD_TAG_CONTEXTS,
               'STANDARD_TAG_TYPES': constants.STANDARD_TAG_TYPES
               }

        return out, json_file

    @authenticated_only
    def on_register_service(self, service_data, client_info):
        service = Service(service_data)

        if not datastore.service_delta.get_if_exists(service.name):
            # Save service delta
            datastore.service_delta.save(service.name, {'version': service.version})
            datastore.service_delta.commit()
            LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                        f"New service registered: {service.name}_{service.version}")

        if not datastore.service.get_if_exists(f'{service.name}_{service.version}'):
            # Save service
            datastore.service.save(f'{service.name}_{service.version}', service)
            datastore.service.commit()
            LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                        f"New service version registered: {service.name}_{service.version}")

    @authenticated_only
    def on_start_download(self, sha256, file_path, client_info):
        LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                    f"Sending file to client, SHA256: {sha256}")

        self.socketio.start_background_task(target=self.send_file, sha256=sha256, file_path=file_path, client_info=client_info)

    @authenticated_only
    def on_upload_file(self, data, classification, sha256, ttl, client_info):
        service_name = client_info['service_name']
        LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                    f"Received file from client, SHA256: {sha256}")

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

    def send_file(self, sha256, file_path, client_info):
        temp_file = tempfile.NamedTemporaryFile()
        try:
            filestore.download(sha256, temp_file.name)
            file_size = os.path.getsize(temp_file.name)

            offset = 0
            chunk_size = 64 * 1024
            with open(temp_file.name, 'rb') as f:
                last_chunk = False
                for chunk in read_in_chunks(f, chunk_size):
                    if (file_size < chunk_size) or ((offset + chunk_size) >= file_size):
                        last_chunk = True

                    self.socketio.emit('write_file_chunk', (file_path, offset, chunk, last_chunk), namespace=self.namespace, room=client_info['id'])
                    offset += chunk_size
        finally:
            temp_file.close()


def read_in_chunks(file_object, chunk_size):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data
