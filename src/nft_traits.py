"""
NFT trait helpers used by main.py for per-trait / per-category gating.

These functions query on-chain data and return integer counts that the bot
compares against a configured threshold.  A return value of None means the
check could not be completed (e.g. API error); the caller treats None as
"pass" so users are never penalised for transient network failures.
"""
from __future__ import annotations

import logging
from typing import Optional


def get_user_nft_trait_count(
    wallet_addresses: list[str],
    collection_id: str,
    trait_name: str,
    trait_value: str,
) -> Optional[int]:
    """Return the total number of NFTs owned across *wallet_addresses* that
    belong to *collection_id* and have ``trait_name == trait_value``.

    Returns ``None`` on any error so the caller can apply a safe fallback.
    """
    # TODO: implement on-chain trait query via SUI RPC / indexer.
    logging.warning("get_user_nft_trait_count: not yet implemented, returning None")
    return None


def get_user_nft_category_count(
    wallet_addresses: list[str],
    collection_id: str,
    trait_name: str,
) -> Optional[int]:
    """Return the total number of NFTs owned across *wallet_addresses* that
    belong to *collection_id* and have any value for *trait_name*.

    Returns ``None`` on any error so the caller can apply a safe fallback.
    """
    # TODO: implement on-chain category query via SUI RPC / indexer.
    logging.warning("get_user_nft_category_count: not yet implemented, returning None")
    return None


def check_nft_trait_ownership(
    wallet_addresses: list[str],
    collection_id: str,
    trait_name: str,
    trait_value: str,
) -> bool:
    """Return ``True`` if any wallet in *wallet_addresses* owns at least one
    NFT from *collection_id* with ``trait_name == trait_value``.

    Returns ``True`` (safe pass) on any error.
    """
    try:
        count = get_user_nft_trait_count(
            wallet_addresses, collection_id, trait_name, trait_value
        )
        if count is None:
            return True  # safe fallback on API failure
        return count > 0
    except Exception:
        logging.exception("check_nft_trait_ownership error")
        return True
