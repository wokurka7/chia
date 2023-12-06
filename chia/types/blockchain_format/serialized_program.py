from __future__ import annotations

from typing import Type

from chia.types.blockchain_format.program import Program



class SerializedProgram(Program):
    """
    An opaque representation of a clvm program. It has a more limited interface than a full SExp
    """

    @property
    def _buf(self):
        return bytes(self)

    @classmethod
    def from_program(cls: Type[SerializedProgram], p: Program) -> SerializedProgram:
        return cls.to(p)

    def to_program(self) -> Program:
        return self
