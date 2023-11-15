from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from tests.util.misc import TestId

# NOTE: When using this module do not import the attributes directly, rather import
#       something like `from tests import ether`.  Importing attributes direclty will
#       result in you likely getting the default `None` values since they are not
#       populated until tests are running.

project_root: Optional[Path] = None
record_property: Optional[Callable[[str, object], None]] = None
test_id: Optional[TestId] = None
