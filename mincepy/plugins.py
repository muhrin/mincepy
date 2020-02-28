import logging
from typing import List

import stevedore

logger = logging.getLogger(__name__)


def get_types() -> List:
    """Get all mincepy types and type helper instances registered as extensions"""
    mgr = stevedore.extension.ExtensionManager(
        namespace='mincepy.plugins.types',
        invoke_on_load=False,
    )

    all_types = []

    def get_type(extension: stevedore.extension.Extension):
        try:
            all_types.extend(extension.plugin())
        except Exception:
            logger.exception("Failed to get types plugin from %s", extension.name)

    mgr.map(get_type)

    return all_types
