from typing import Dict

from assemblyline.odm.models.heuristic import Heuristic
from assemblyline_service_server.config import STORAGE


def get_heuristics() -> Dict[str, Heuristic]:
    return {h.heur_id: h for h in STORAGE.list_all_heuristics()}
