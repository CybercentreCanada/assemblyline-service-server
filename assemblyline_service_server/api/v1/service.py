import copy

from flask import request

from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.service import Service
from assemblyline_service_server.api.base import make_subapi_blueprint, make_api_response, api_login
from assemblyline_service_server.config import LOGGER, STORAGE

SUB_API = 'service'
service_api = make_subapi_blueprint(SUB_API, api_version=1)
service_api._doc = "Perform operations on service"


@service_api.route("/register/", methods=["PUT", "POST"])
@api_login()
def register_service(client_info):
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
        # Get heuristics list
        heuristics = data.pop('heuristics', None)

        # Pop unused registration data
        for x in ['file_required', 'tool_version']:
            data.pop(x, None)

        # Create Service registration object
        service = Service(data)
        keep_alive = True

        # Save service if it doesn't already exist
        if not STORAGE.service.get_if_exists(f'{service.name}_{service.version}'):
            STORAGE.service.save(f'{service.name}_{service.version}', service)
            STORAGE.service.commit()
            LOGGER.info(f"{client_info['client_id']} - {client_info['service_name']} registered")
            keep_alive = False

        # Save service delta if it doesn't already exist
        if not STORAGE.service_delta.get_if_exists(service.name):
            STORAGE.service_delta.save(service.name, {'version': service.version})
            STORAGE.service_delta.commit()
            LOGGER.info(f"{client_info['client_id']} - {client_info['service_name']} "
                        f"version ({service.version}) registered")

        new_heuristics = []
        if heuristics:
            plan = STORAGE.heuristic.get_bulk_plan()
            for index, heuristic in enumerate(heuristics):
                heuristic_id = f'#{index}'  # Set heuristic id to it's position in the list for logging purposes
                try:
                    # Append service name to heuristic ID
                    heuristic['heur_id'] = f"{service.name.upper()}.{str(heuristic['heur_id'])}"

                    heuristic = Heuristic(heuristic)
                    heuristic_id = heuristic.heur_id
                    plan.add_upsert_operation(heuristic_id, heuristic)
                except Exception as e:
                    LOGGER.exception(f"{client_info['client_id']} - {client_info['service_name']} "
                                     f"invalid heuristic ({heuristic_id}) ignored: {str(e)}")
                    raise ValueError("Error parsing heuristics")

            res = STORAGE.heuristic.bulk(plan)
            # TODO: parse res
            STORAGE.heuristic.commit()

        service_config = STORAGE.get_service_with_delta(service.name, as_obj=False)

    except ValueError as e:  # Catch errors when building Service or Heuristic model(s)
        return make_api_response("", err=e, status_code=400)

    return make_api_response(dict(
        keep_alive=keep_alive,
        new_heuristics=new_heuristics,
        service_config=service_config or dict(),
    ))
