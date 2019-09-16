import copy

from flask import request

from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.service import Service
from assemblyline_service_server.api.base import make_subapi_blueprint, make_api_response, api_login
from assemblyline_service_server.config import LOGGER, STORAGE

SUB_API = 'service'
service_api = make_subapi_blueprint(SUB_API, api_version=1)
service_api._doc = "Perform operations on service"


@service_api.route("/register/", methods=["PUT"])
@api_login()
def register_service():
    """

    Data Block:
    {
    TODO: service manifest
    }

    Result example:
    {'keep_alive': true}
    """
    data = request.json

    try:
        service = copy.deepcopy(data)
        # Pop the data not part of service model
        for x in ['file_required', 'tool_version', 'heuristics']:
            service.pop(x, None)

        service = Service(service)
        keep_alive = True

        # Save service if it doesn't already exist
        if not STORAGE.service.get_if_exists(f'{service.name}_{service.version}'):
            STORAGE.service.save(f'{service.name}_{service.version}', service)
            STORAGE.service.commit()
            LOGGER.info(f"New service registered: {service.name}")
            keep_alive = False

        # Save service delta if it doesn't already exist
        if not STORAGE.service_delta.get_if_exists(service.name):
            STORAGE.service_delta.save(service.name, {'version': service.version})
            STORAGE.service_delta.commit()
            LOGGER.info(f"New {service.name} version registered: {service.version}")

        heuristics = data.get('heuristics', None)
        new_heuristics = []
        if heuristics:
            for index, heuristic in enumerate(heuristics):
                heuristic_id = f'#{index}'  # Assign a safe name for the heuristic in case parsing fails
                try:
                    heuristic = Heuristic(heuristic)
                    heuristic_id = heuristic.heur_id
                    if not STORAGE.heuristic.get_if_exists(heuristic.heur_id):
                        STORAGE.heuristic.save(heuristic.heur_id, heuristic)
                        STORAGE.heuristic.commit()
                        new_heuristics.append(heuristic.heur_id)
                        LOGGER.info(f"New {service.name} heuristic saved: {heuristic.heur_id}")
                except Exception as e:
                    LOGGER.warning(f"Ignoring invalid {service.name} heuristic ({heuristic_id}): {str(e)}")
    except ValueError as e:  # Catch errors when building Service or Heuristic model(s)
        return make_api_response("", str(e), 400)

    return make_api_response(dict(
        keep_alive=keep_alive,
        new_heuristics=new_heuristics,
    ))
