from typing import Optional

import attr


@attr.s(auto_attribs=True)
class RefereeCommand:
    state: int
    commandForTeam: int
    isPartOfFieldLeft: bool
    ball_pos: Optional[tuple[float, float]] = None
