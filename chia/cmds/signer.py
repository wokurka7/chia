from __future__ import annotations

import asyncio
import inspect
import sys
from dataclasses import Field, dataclass, field, fields, replace
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Coroutine, Dict, Iterable, List, Optional, Type, TypeVar, Union

import click
from chia_rs import AugSchemeMPL, G2Element

from chia.cmds.cmds_util import TransactionBundle, get_wallet_client
from chia.rpc.util import ALL_TRANSPORT_LAYERS
from chia.rpc.wallet_request_types import ApplySignatures, ExecuteSigningInstructions, GatherSigningInfo
from chia.rpc.wallet_rpc_client import WalletRpcClient
from chia.types.spend_bundle import SpendBundle
from chia.wallet.signer_protocol import SignedTransaction, SigningInstructions, SigningResponse, Spend
from chia.wallet.transaction_record import TransactionRecord
from chia.wallet.util.clvm_streamable import ClvmStreamable, clvm_serialization_mode


# TODO: Make this pattern better and propogate it
@dataclass(frozen=True)
class WalletClientInfo:
    client: WalletRpcClient
    fingerprint: int
    config: Dict[str, Any]


SyncOrAsyncCmd = Callable[..., Union[None, Coroutine[Any, Any, None]]]
SyncCmd = Callable[..., None]


_T_CMDBase = TypeVar("_T_CMDBase", bound="CMDBase")


@dataclass(frozen=True)
class CMDBase:
    @staticmethod
    def name() -> str:
        raise NotImplementedError()

    @staticmethod
    def help() -> str:
        raise NotImplementedError()

    @classmethod
    def get_base_cmd(cls: Type[_T_CMDBase]) -> SyncOrAsyncCmd:
        def base_cmd(**kwargs: Any) -> None:
            cls(**kwargs).run()

        return base_cmd

    @classmethod
    def _recurse_wrap(cls: Type[_T_CMDBase], cmd: SyncOrAsyncCmd, _fields: Iterable[Field[Any]]) -> SyncOrAsyncCmd:
        wrapped_cmd = cmd
        for klass in cls.__bases__:
            if not issubclass(klass, CMDBase):
                continue

            # Semantics guarantee that klass is a CMDBase and thus has this member function
            wrapped_cmd = klass.wrap_cmd(wrapped_cmd, _fields)  # pylint: disable=no-member

        return wrapped_cmd

    @classmethod
    def wrap_cmd(cls: Type[_T_CMDBase], cmd: SyncOrAsyncCmd, _fields: Iterable[Field[Any]]) -> SyncOrAsyncCmd:
        wrapped_cmd = cmd
        for _field in _fields:
            if "param_decls" not in _field.metadata:
                continue
            wrapped_cmd = click.option(
                *_field.metadata["param_decls"], **{k: v for k, v in _field.metadata.items() if k != "param_decls"}
            )(wrapped_cmd)

        return wrapped_cmd

    @classmethod
    def get_cmd_func(cls: Type[_T_CMDBase]) -> SyncOrAsyncCmd:
        return cls.wrap_cmd(cls.get_base_cmd(), fields(cls))

    def run(self) -> None:
        raise NotImplementedError()


_T_AsyncCMDBase = TypeVar("_T_AsyncCMDBase", bound="AsyncCMDBase")


@dataclass(frozen=True)
class AsyncCMDBase(CMDBase):
    @classmethod
    def get_base_cmd(cls: Type[_T_AsyncCMDBase]) -> SyncOrAsyncCmd:
        async def base_cmd(**kwargs: Any) -> None:
            await cls(**kwargs).async_run()

        return base_cmd

    @classmethod
    def wrap_cmd(cls: Type[_T_CMDBase], cmd: SyncOrAsyncCmd, _fields: Iterable[Field[Any]]) -> SyncOrAsyncCmd:
        def wrap_for_sync(func: SyncOrAsyncCmd) -> SyncOrAsyncCmd:
            def new_func(**kwargs: Any) -> None:
                coro = func(**kwargs)
                assert coro is not None
                asyncio.run(coro)

            return new_func

        return cls._recurse_wrap(wrap_for_sync(cmd), _fields)

    async def async_run(self) -> None:
        raise NotImplementedError()


@dataclass(frozen=True)
class NeedsWalletRPC:
    wallet_rpc: WalletClientInfo
    wallet_rpc_port: int = field(
        metadata=dict(
            param_decls=("-wp", "--wallet-rpc_port"),
            help=(
                "Set the port where the Wallet is hosting the RPC interface."
                "See the rpc_port under wallet in config.yaml."
            ),
            type=int,
            default=None,
        )
    )
    fingerprint: int = field(
        metadata=dict(
            param_decls=("-f", "--fingerprint"),
            help="Fingerprint of the wallet to use",
            type=int,
            default=None,
        )
    )

    # The type ignore here is because NeedsWalletRPC is not meant actually to be a command on its own.
    # This problably suggests an issue with this pattern but we'll just ignore and move on for now
    @classmethod
    def wrap_cmd(  # type: ignore[misc]
        cls: Type[_T_AsyncCMDBase],
        cmd: SyncOrAsyncCmd,
        _fields: Iterable[Field[Any]],
    ) -> SyncOrAsyncCmd:
        def wrap_for_wallet_client(func: SyncOrAsyncCmd) -> SyncOrAsyncCmd:
            async def new_func(
                wallet_rpc_port: Optional[int] = None, fingerprint: Optional[int] = None, **kwargs: Any
            ) -> None:
                async with get_wallet_client(wallet_rpc_port, fingerprint) as (wallet_client, fp, config):
                    coro = func(
                        wallet_rpc=WalletClientInfo(wallet_client, fp, config),
                        wallet_rpc_port=wallet_client.port,
                        fingerprint=fp,
                        **kwargs,
                    )
                    assert coro is not None
                    await coro

            return new_func

        return cls._recurse_wrap(wrap_for_wallet_client(cmd), _fields)  # pylint: disable=no-member


class WalletCMD:
    pass


def register_signer(wallet_cmd: click.Group) -> None:
    @wallet_cmd.group("signer", help="Get information for an external signer")
    def signer_cmd() -> None:
        pass

    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and issubclass(obj, CMDBase) and obj not in (CMDBase, AsyncCMDBase):
            if issubclass(obj, WalletCMD):
                thing_to_attach_to = wallet_cmd
            else:
                thing_to_attach_to = signer_cmd
            thing_to_attach_to.command(obj.name(), help=obj.help())(obj.get_cmd_func())


@dataclass(frozen=True)
class TransactionsIn:
    transaction_file_in: str = field(
        metadata=dict(
            param_decls=("--transaction-file-in", "-i"),
            type=str,
            help="Transaction file to use as input",
            required=True,
        )
    )

    @cached_property
    def transaction_bundle(self) -> TransactionBundle:
        with open(Path(self.transaction_file_in), "rb") as file:
            return TransactionBundle.from_bytes(file.read())


@dataclass(frozen=True)
class TransactionsOut:
    transaction_file_out: str = field(
        metadata=dict(
            param_decls=("--transaction-file-out", "-o"),
            type=str,
            help="Transaction filename to use as output",
            required=True,
        )
    )

    def handle_transaction_output(self, output: List[TransactionRecord]) -> None:
        with open(Path(self.transaction_file_out), "wb") as file:
            file.write(bytes(TransactionBundle(output)))


@dataclass(frozen=True)
class _SPCompression:
    compression: str = field(
        metadata=dict(
            param_decls=("--compression", "-c"),
            type=click.Choice(["none", "chip-TBD"]),
            default="none",
            help="Wallet Signer Protocol CHIP to use for compression of output",
        ),
    )


_T_ClvmStreamable = TypeVar("_T_ClvmStreamable", bound=ClvmStreamable)


@dataclass(frozen=True)
class SPIn(_SPCompression):
    signer_protocol_input: List[str] = field(
        metadata=dict(
            param_decls=("--signer-protocol-input", "-p"),
            type=str,
            help="Signer protocol objects (signatures, signing instructions, etc.) as files to load as input",
            multiple=True,
            required=True,
        )
    )

    def read_sp_input(self, typ: Type[_T_ClvmStreamable]) -> List[_T_ClvmStreamable]:
        final_list: List[_T_ClvmStreamable] = []
        for filename in self.signer_protocol_input:
            with open(Path(filename), "rb") as file:
                with clvm_serialization_mode(
                    True, ALL_TRANSPORT_LAYERS[self.compression] if self.compression != "none" else None
                ):
                    final_list.append(typ.from_bytes(file.read()))

        return final_list


@dataclass(frozen=True)
class SPOut(_SPCompression):
    output_format: str = field(
        metadata=dict(
            param_decls=("--output-format", "-t"),
            type=click.Choice(["hex", "file"]),
            default="hex",
            help="How to output the information to transfer to an external signer",
        ),
    )
    output_file: List[str] = field(
        metadata=dict(
            param_decls=("--output-file", "-b"),
            type=str,
            multiple=True,
            help="The file to output to (if --output-format=file)",
        ),
    )

    def handle_clvm_output(self, outputs: List[ClvmStreamable]) -> None:
        with clvm_serialization_mode(
            True, ALL_TRANSPORT_LAYERS[self.compression] if self.compression != "none" else None
        ):
            if self.output_format == "hex":
                for output in outputs:
                    print(bytes(output).hex())
            if self.output_format == "file":
                if len(self.output_file) == 0:
                    print("--output-format=file specifed without any --output-file")
                    return
                elif len(self.output_file) != len(outputs):
                    print(
                        "Incorrect number of file outputs specified, "
                        f"expected: {len(outputs)} got {len(self.output_file)}"
                    )
                    return
                else:
                    for filename, output in zip(self.output_file, outputs):
                        with open(Path(filename), "wb") as file:
                            file.write(bytes(output))


@dataclass(frozen=True)
class GatherSigningInfoCMD(SPOut, TransactionsIn, NeedsWalletRPC, AsyncCMDBase):
    @staticmethod
    def name() -> str:
        return "gather_signing_info"

    @staticmethod
    def help() -> str:
        return "Gather the information from a transaction that a signer needs in order to create a signature"

    async def async_run(self) -> None:
        spends: List[Spend] = [
            Spend.from_coin_spend(cs)
            for tx in self.transaction_bundle.txs
            if tx.spend_bundle is not None
            for cs in tx.spend_bundle.coin_spends
        ]
        signing_instructions: SigningInstructions = (
            await self.wallet_rpc.client.gather_signing_info(GatherSigningInfo(spends=spends))
        ).signing_instructions
        self.handle_clvm_output([signing_instructions])


@dataclass(frozen=True)
class ApplySignaturesCMD(TransactionsOut, SPIn, TransactionsIn, NeedsWalletRPC, AsyncCMDBase):
    @staticmethod
    def name() -> str:
        return "apply_signatures"

    @staticmethod
    def help() -> str:
        return "Apply a signer's signatures to a transaction bundle"

    async def async_run(self) -> None:
        signing_responses: List[SigningResponse] = self.read_sp_input(SigningResponse)
        spends: List[Spend] = [
            Spend.from_coin_spend(cs)
            for tx in self.transaction_bundle.txs
            if tx.spend_bundle is not None
            for cs in tx.spend_bundle.coin_spends
        ]
        signed_transactions: List[SignedTransaction] = (
            await self.wallet_rpc.client.apply_signatures(
                ApplySignatures(spends=spends, signing_responses=signing_responses)
            )
        ).signed_transactions
        signed_spends: List[Spend] = [spend for tx in signed_transactions for spend in tx.transaction_info.spends]
        final_signature: G2Element = G2Element()
        for signature in [sig for tx in signed_transactions for sig in tx.signatures]:
            if signature.type != "bls_12381_aug_scheme":
                print("No external spot for non BLS signatures in a spend")
                return
            final_signature = AugSchemeMPL.aggregate([final_signature, G2Element.from_bytes(signature.signature)])
        new_spend_bundle: SpendBundle = SpendBundle([spend.as_coin_spend() for spend in signed_spends], final_signature)
        new_transactions: List[TransactionRecord] = [
            replace(self.transaction_bundle.txs[0], spend_bundle=new_spend_bundle, name=new_spend_bundle.name()),
            *(replace(tx, spend_bundle=None) for tx in self.transaction_bundle.txs),
        ]
        self.handle_transaction_output(new_transactions)


@dataclass(frozen=True)
class ExecuteSigningInstructionsCMD(SPOut, SPIn, NeedsWalletRPC, AsyncCMDBase):
    @staticmethod
    def name() -> str:
        return "execute_signing_instructions"

    @staticmethod
    def help() -> str:
        return "Given some signing instructions, return signing responses"

    async def async_run(self) -> None:
        signing_instructions: List[SigningInstructions] = self.read_sp_input(SigningInstructions)
        self.handle_clvm_output(
            [
                signing_response
                for instruction_set in signing_instructions
                for signing_response in (
                    await self.wallet_rpc.client.execute_signing_instructions(
                        ExecuteSigningInstructions(signing_instructions=instruction_set, partial_allowed=True)
                    )
                ).signing_responses
            ]
        )


@dataclass(frozen=True)
class PushTransactionsCMD(TransactionsIn, NeedsWalletRPC, AsyncCMDBase, WalletCMD):
    @staticmethod
    def name() -> str:
        return "push_transactions"

    @staticmethod
    def help() -> str:
        return "Push a transaction bundle to the wallet to send to the network"

    async def async_run(self) -> None:
        await self.wallet_rpc.client.push_transactions(self.transaction_bundle.txs)
