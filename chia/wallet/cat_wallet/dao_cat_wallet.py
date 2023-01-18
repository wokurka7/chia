from __future__ import annotations

import dataclasses
import logging
import time
import traceback
from secrets import token_bytes
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING
from chia.types.announcement import Announcement
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend
from chia.util.ints import uint8, uint32, uint64, uint128
from chia.wallet.util.wallet_types import WalletType
from blspy import AugSchemeMPL, G2Element, G1Element
from chia.wallet.cat_wallet.cat_utils import (
    SpendableCAT,
    construct_cat_puzzle,
    match_cat_puzzle,
    unsigned_spend_bundle_for_spendable_cats,
)
from chia.util.byte_types import hexstr_to_bytes
from chia.wallet.puzzles.cat_loader import CAT_MOD
from chia.wallet.cat_wallet.cat_wallet import CATWallet
from chia.wallet.cat_wallet.dao_cat_info import DAOCATInfo
from chia.wallet.dao_wallet.dao_wallet import DAOWallet
from chia.wallet.coin_selection import select_coins
from chia.wallet.dao_wallet.dao_utils import get_lockup_puzzle
from chia.wallet.cat_wallet.lineage_store import CATLineageStore
from chia.wallet.wallet_state_manager import WalletStateManager
from chia.wallet.wallet import Wallet
from chia.wallet.wallet_coin_record import WalletCoinRecord
from chia.wallet.wallet_info import WalletInfo


CAT_MOD_HASH = CAT_MOD.get_tree_hash()


class DAOCATWallet():
    wallet_state_manager: WalletStateManager
    log: logging.Logger
    wallet_info: WalletInfo
    dao_cat_info: DAOCATInfo
    standard_wallet: Wallet
    cost_of_single_tx: Optional[int]
    lineage_store: CATLineageStore

    @classmethod
    def type(cls) -> uint8:
        return uint8(WalletType.DAO_CAT)

    @staticmethod
    async def get_or_create_wallet_for_cat(
        wallet_state_manager: WalletStateManager,
        wallet: Wallet,
        limitations_program_hash_hex: str,
        name: Optional[str] = None,
    ) -> DAOCATWallet:
        self = DAOCATWallet()
        self.cost_of_single_tx = None
        self.standard_wallet = wallet
        self.log = logging.getLogger(__name__)

        limitations_program_hash_hex = bytes32.from_hexstr(limitations_program_hash_hex).hex()  # Normalize the format

        dao_wallet_id = None
        free_cat_wallet_id = None
        for id, w in wallet_state_manager.wallets.items():
            if w.type() == DAOCATWallet.type():
                assert isinstance(w, DAOCATWallet)
                if w.get_asset_id() == limitations_program_hash_hex:
                    self.log.warning("Not creating wallet for already existing DAO CAT wallet")
                    return w
            elif w.type() == CATWallet.type():
                assert isinstance(w, CATWallet)
                if w.get_asset_id() == limitations_program_hash_hex:
                    free_cat_wallet_id = w.id()
        assert free_cat_wallet_id is not None
        for id, w in wallet_state_manager.wallets.items():
            if w.type() == DAOWallet.type():
                assert isinstance(w, DAOWallet)
                if w.get_cat_wallet_id() == free_cat_wallet_id:
                    dao_wallet_id = w.id()
        assert dao_wallet_id is not None
        self.wallet_state_manager = wallet_state_manager
        if name is None:
            name = self.default_wallet_name_for_unknown_cat(limitations_program_hash_hex)

        limitations_program_hash = bytes32(hexstr_to_bytes(limitations_program_hash_hex))

        self.dao_cat_info = DAOCATInfo(
            dao_wallet_id,
            free_cat_wallet_id,
            limitations_program_hash,
            None,
            [],
        )
        info_as_string = bytes(self.dao_cat_info).hex()
        self.wallet_info = await wallet_state_manager.user_store.create_wallet(name, WalletType.DAO_CAT, info_as_string)

        self.lineage_store = await CATLineageStore.create(self.wallet_state_manager.db_wrapper, self.get_asset_id())
        await self.wallet_state_manager.add_new_wallet(self, self.id())
        return self

    # maybe we change this to return the full records and just add the clean ones ourselves later
    async def advanced_select_coins(self, amount: uint64, proposal_id: bytes32) -> Set[Coin]:
        coins = Set()
        sum = 0
        for coin in self.dao_cat_info.locked_coins:
            compatible = True
            for prev_vote in coin.previous_votes:
                if prev_vote == proposal_id:
                    compatible = False
                    break
            if compatible:
                coins.add(coin.coin)
                sum += coin.coin.amount
                if sum >= amount:
                    break
        # try and get already locked up coins first
        if sum >= amount:
            return coins
        coins = await select_coins(amount - sum(c.amount for c in coins))
        assert sum(c.amount for c in coins) >= amount
        # loop through our coins and check which ones haven't yet voted on that proposal and add them to coins set
        return coins

    async def create_vote_spend(amount: uint64, proposal_id: bytes32, is_yes_vote: bool):

        return

    async def enter_vote_state(self, coins: Optional[List[Coin]] = None):
        innerpuz = await self.get_new_inner_puzzle()
        puzzle = get_lockup_puzzle(
            self.cat_info.limitations_program_hash,
            [],
            innerpuz,
        )

        return

    async def exit_vote_state():

        return

    async def add_coin_to_tracked_list():

        return

    async def update_coin_in_tracked_list():

        return

    async def get_asset_id(self):
        return bytes(self.cat_info.limitations_program_hash).hex()