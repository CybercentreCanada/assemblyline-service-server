import os
import tempfile
from typing import List

from assemblyline.common import forge
from assemblyline.common import identify
from assemblyline.common.isotime import now_as_iso
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.service import Service
from assemblyline_service_server.session import ServiceClient
from assemblyline_service_server.sio.base import BaseNamespace, authenticated_only, LOGGER

filestore = forge.get_filestore()
datastore = forge.get_datastore()


class HelperNamespace(BaseNamespace):
    @authenticated_only
    def on_get_classification_definition(self, client_info: ServiceClient):
        LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                    f"Sending classification definition to {client_info.service_name} service client")

        return forge.get_classification().__dict__['original_definition']

    @authenticated_only
    def on_register_service(self, service_data: dict, client_info: ServiceClient):
        keep_alive = True
        service = Service(service_data)

        if not datastore.service.get_if_exists(f'{service.name}_{service.version}'):
            # Save service
            datastore.service.save(f'{service.name}_{service.version}', service)
            datastore.service.commit()
            LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                        f"New service version registered: {service.name}_{service.version}")
            keep_alive = False

        if not datastore.service_delta.get_if_exists(service.name):
            # Save service delta
            datastore.service_delta.save(service.name, {'version': service.version})
            datastore.service_delta.commit()
            LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                        f"New service registered: {service.name}_{service.version}")

        return keep_alive

    @authenticated_only
    def on_save_heuristics(self, heuristics: List[dict], client_info: ServiceClient):
        new_heuristic = False

        for index, heuristic in enumerate(heuristics):
            heuristic_id = f'#{index}'  # Assign a safe name for the heuristic in case parsing fails
            try:
                heuristic = Heuristic(heuristic)
                heuristic_id = heuristic.heur_id
                if not datastore.heuristic.get_if_exists(heuristic.heur_id):
                    datastore.heuristic.save(heuristic.heur_id, heuristic)
                    datastore.heuristic.commit()
                    LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                                f"New {client_info.service_name} service Heuristic saved: {heuristic.heur_id}")
                    new_heuristic = True
            except Exception as e:
                LOGGER.error(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                             f"{client_info.service_name} service tried to register an invalid Heuristic "
                             f"({heuristic_id}): {str(e)}")
                return False

        return new_heuristic

    @authenticated_only
    def on_start_download(self, sha256: str, file_path: str, client_info: ServiceClient):
        LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                    f"Sending file to {client_info.service_name} service client, SHA256: {sha256}")

        self.socketio.start_background_task(target=self.send_file, sha256=sha256, file_path=file_path, client_info=client_info)

    @authenticated_only
    def on_file_exists(self, sha256: str, src_path: str, classification: str, ttl: int, client_info: ServiceClient):
        if not filestore.exists(sha256):
            LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                        f"New file from {client_info.service_name} service client doesn't exist, SHA256: {sha256}")

            # Create a temp folder to save uploaded file to
            temp_upload_folder = os.path.join(tempfile.gettempdir(), 'uploads')
            if not os.path.isdir(temp_upload_folder):
                os.makedirs(temp_upload_folder)

            # Create empty file to prepare for uploading the file in chunks
            dest_path = os.path.join(temp_upload_folder, sha256)
            with open(dest_path, 'wb'):
                pass

            return sha256, src_path, classification, ttl

        return None, None, None, None

    @authenticated_only
    def on_upload_file_chunk(self, offset: int, chunk: bytes, last_chunk: bool, classification: str,
                             sha256: str, ttl: int, client_info: ServiceClient):
        dest_path = os.path.join(tempfile.gettempdir(), 'uploads', sha256)
        try:
            with open(dest_path, 'r+b') as f:
                f.seek(offset)
                f.write(chunk)

            # Let the client know that the chunk was written successfully
            self.socketio.emit('chunk_upload_success', True, namespace=self.namespace, room=client_info.client_id)

            if last_chunk:
                file_info = identify.fileinfo(dest_path)

                # Validate SHA256 of the received file
                if sha256 != file_info['sha256']:
                    if os.path.isfile(dest_path):
                        os.unlink(dest_path)
                    # TODO: retry upload
                    # self.socketio.emit('upload_success', sha256, file_path, result['classification'], task.ttl), namespace=self.namespace, room=client_info.client_id)
                    LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                                f"SHA256 of received file from {client_info.service_name} service client doesn't match: "
                                f"{sha256} != {file_info['sha256']}")
                else:
                    file_info['classification'] = classification
                    file_info['expiry_ts'] = now_as_iso(ttl * 24 * 60 * 60)
                    datastore.save_or_freshen_file(file_info['sha256'], file_info, file_info['expiry_ts'],
                                                   file_info['classification'])
                    if not filestore.exists(file_info['sha256']):
                        filestore.upload(dest_path, file_info['sha256'])

                    self.socketio.emit('upload_success', True, namespace=self.namespace, room=client_info.client_id)
                    LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                                f"Successfully received file from {client_info.service_name} service client, SHA256: {sha256}")

                    if os.path.isfile(dest_path):
                        os.unlink(dest_path)

        except IOError as e:
            LOGGER.error(f"An error occurred while downloading file to: {dest_path}")
            LOGGER.error(str(e))
            if os.path.isfile(dest_path):
                os.unlink(dest_path)

    def send_file(self, sha256: str, file_path: str, client_info: ServiceClient):
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

                    self.socketio.emit('write_file_chunk', (file_path, offset, chunk, last_chunk), namespace=self.namespace, room=client_info.client_id)
                    offset += chunk_size
        finally:
            temp_file.close()


def read_in_chunks(file_object, chunk_size: int):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data
