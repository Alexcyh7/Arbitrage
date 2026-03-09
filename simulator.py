import copy
import math
from decimal import Decimal, getcontext, ROUND_DOWN

getcontext().prec = 256

Q96 = Decimal(2) ** 96
INT24_THRESHOLD = 8388607  # 2^23 - 1
INT24_MOD = 16777216       # 2^24


# ──────────────────────────────────────────────
#  Uniswap V2
# ──────────────────────────────────────────────

def _v2_swap(pool: dict, input_token: str, amount_in: int):
    """Core V2 constant-product swap. Returns (amount_out, token0_is_input)."""
    state = pool["poolState"]["poolState"]
    static = pool["poolState"]["poolStaticInfo"]

    token0 = static["token0"].lower()
    token1 = static["token1"].lower()
    input_token_lower = input_token.lower()

    if input_token_lower == token0:
        reserve_in = int(state["tokenBalance0"])
        reserve_out = int(state["tokenBalance1"])
        zero_for_one = True
    elif input_token_lower == token1:
        reserve_in = int(state["tokenBalance1"])
        reserve_out = int(state["tokenBalance0"])
        zero_for_one = False
    else:
        raise ValueError(f"input_token {input_token} not in pool")

    fee = int(static["swapFee"])  # e.g. 3000 means 0.3%
    # amount_in_with_fee = amount_in * (1_000_000 - fee)
    # amount_out = amount_in_with_fee * reserve_out / (reserve_in * 1_000_000 + amount_in_with_fee)
    amount_in_with_fee = amount_in * (1_000_000 - fee)
    numerator = amount_in_with_fee * reserve_out
    denominator = reserve_in * 1_000_000 + amount_in_with_fee
    amount_out = numerator // denominator

    return amount_out, zero_for_one


def get_v2_amount_out(pool: dict, input_token: str, amount_in: int) -> int:
    """
    Compute output amount for a Uniswap V2 swap.

    Args:
        pool: A V2 pool dict (element from filtered-v2pools.json).
        input_token: Address of the input token (must match token0 or token1).
        amount_in: Input amount in raw token units (int).

    Returns:
        Output amount in raw token units (int).
    """
    amount_out, _ = _v2_swap(pool, input_token, amount_in)
    return amount_out


def get_v2_amount_out_with_pool_state(pool: dict, input_token: str, amount_in: int) -> tuple:
    """
    Compute output amount and return updated pool state for a V2 swap.

    Returns:
        (amount_out, updated_pool) where updated_pool is a deep copy with
        adjusted token balances.
    """
    amount_out, zero_for_one = _v2_swap(pool, input_token, amount_in)

    updated_pool = copy.deepcopy(pool)
    state = updated_pool["poolState"]["poolState"]

    if zero_for_one:
        state["tokenBalance0"] = str(int(state["tokenBalance0"]) + amount_in)
        state["tokenBalance1"] = str(int(state["tokenBalance1"]) - amount_out)
    else:
        state["tokenBalance1"] = str(int(state["tokenBalance1"]) + amount_in)
        state["tokenBalance0"] = str(int(state["tokenBalance0"]) - amount_out)

    return amount_out, updated_pool


# ──────────────────────────────────────────────
#  Uniswap V3 helpers
# ──────────────────────────────────────────────

def _decode_tick(tick_val) -> int:
    """Decode currentTick from unsigned int24 representation to signed."""
    t = int(tick_val)
    if t > INT24_THRESHOLD:
        t -= INT24_MOD
    return t


def _sqrt_price_x96_to_decimal(sqrt_price_x96: int) -> Decimal:
    return Decimal(sqrt_price_x96) / Q96


def _decimal_to_sqrt_price_x96(sp: Decimal) -> int:
    return int((sp * Q96).to_integral(rounding=ROUND_DOWN))


def _tick_from_price(price: Decimal) -> int:
    if price <= 0:
        return -887272
    return int(math.floor(math.log(float(price)) / math.log(1.0001)))


def _sqrt_price_from_tick(tick: int) -> Decimal:
    return (Decimal('1.0001') ** (Decimal(tick) / 2)).quantize(
        Decimal('1e-18'), rounding=ROUND_DOWN
    )


# ──────────────────────────────────────────────
#  Uniswap V3 swap engine
# ──────────────────────────────────────────────

def _v3_swap(pool: dict, input_token: str, amount_in: int):
    """
    Core V3 tick-based swap simulation.

    Returns: (amount_out, updated_pool_state_dict, zero_for_one)
    """
    pool_state = pool["poolState"]["poolState"]
    static = pool["poolState"]["poolStaticInfo"]

    token0 = static["token0"].lower()
    token1 = static["token1"].lower()
    input_lower = input_token.lower()

    if input_lower == token0:
        zero_for_one = True
    elif input_lower == token1:
        zero_for_one = False
    else:
        raise ValueError(f"input_token {input_token} not in pool")

    fee_rate = Decimal(static["swapFee"]) / Decimal(1_000_000)
    sp_current = _sqrt_price_x96_to_decimal(int(pool_state["sqrtPriceX96"]))
    if sp_current <= Decimal('1e-18'):
        sp_current = Decimal('1e-18')

    L = Decimal(pool_state["liquidity"])
    current_tick = _decode_tick(pool_state["currentTick"])
    amount_remaining = Decimal(amount_in)
    amount_out = Decimal(0)
    EPSILON = Decimal('1e-18')

    if zero_for_one:
        # token0 -> token1: price decreases, walk ticks downward
        ticks = sorted(
            [t for t in pool_state["tickBitMap"] if int(t[0]) <= current_tick],
            key=lambda x: -int(x[0])
        )

        for tick_info in ticks:
            if amount_remaining <= 0:
                break

            tick_idx = int(tick_info[0])
            tick_data = tick_info[1]
            sp_target = _sqrt_price_from_tick(tick_idx)
            if sp_target >= sp_current:
                continue

            # How much token0 (net of fee) needed to reach this tick
            if L < EPSILON:
                break
            dx_net = L * (sp_current - sp_target) / (sp_current * sp_target)
            dx_gross = dx_net / (Decimal(1) - fee_rate)

            if amount_remaining >= dx_gross:
                # Consume entire tick range
                dy = L * (sp_current - sp_target)
                amount_out += dy
                amount_remaining -= dx_gross
                sp_current = sp_target
                if tick_data["initialized"]:
                    L -= Decimal(tick_data["liquidityNet"])
            else:
                # Partial fill within this tick range
                dx_net_avail = amount_remaining * (Decimal(1) - fee_rate)
                if L < EPSILON:
                    break
                sp_new = (L * sp_current) / (L + dx_net_avail * sp_current)
                sp_new = sp_new.quantize(Decimal('1e-18'), rounding=ROUND_DOWN)
                amount_out += L * (sp_current - sp_new)
                amount_remaining = Decimal(0)
                sp_current = sp_new

        # Remaining amount after all ticks
        if amount_remaining > 0 and L >= EPSILON:
            dx_net_avail = amount_remaining * (Decimal(1) - fee_rate)
            sp_new = (L * sp_current) / (L + dx_net_avail * sp_current)
            sp_new = sp_new.quantize(Decimal('1e-18'), rounding=ROUND_DOWN)
            amount_out += L * (sp_current - sp_new)
            sp_current = sp_new

    else:
        # token1 -> token0: price increases, walk ticks upward
        ticks = sorted(
            [t for t in pool_state["tickBitMap"] if int(t[0]) > current_tick],
            key=lambda x: int(x[0])
        )

        for tick_info in ticks:
            if amount_remaining <= 0:
                break

            tick_idx = int(tick_info[0])
            tick_data = tick_info[1]
            sp_target = _sqrt_price_from_tick(tick_idx)
            if sp_target <= sp_current:
                continue

            if L < EPSILON:
                break
            dy_net = L * (sp_target - sp_current)
            dy_gross = dy_net / (Decimal(1) - fee_rate)

            if amount_remaining >= dy_gross:
                dx = L * (Decimal(1) / sp_current - Decimal(1) / sp_target)
                amount_out += dx
                amount_remaining -= dy_gross
                sp_current = sp_target
                if tick_data["initialized"]:
                    L += Decimal(tick_data["liquidityNet"])
            else:
                dy_net_avail = amount_remaining * (Decimal(1) - fee_rate)
                if L < EPSILON:
                    break
                sp_new = sp_current + dy_net_avail / L
                sp_new = sp_new.quantize(Decimal('1e-18'), rounding=ROUND_DOWN)
                amount_out += L * (Decimal(1) / sp_current - Decimal(1) / sp_new)
                amount_remaining = Decimal(0)
                sp_current = sp_new

        if amount_remaining > 0 and L >= EPSILON:
            dy_net_avail = amount_remaining * (Decimal(1) - fee_rate)
            sp_new = sp_current + dy_net_avail / L
            sp_new = sp_new.quantize(Decimal('1e-18'), rounding=ROUND_DOWN)
            amount_out += L * (Decimal(1) / sp_current - Decimal(1) / sp_new)
            sp_current = sp_new

    # Build updated pool state
    updated_pool = copy.deepcopy(pool)
    us = updated_pool["poolState"]["poolState"]
    us["sqrtPriceX96"] = str(_decimal_to_sqrt_price_x96(sp_current))
    new_tick = _tick_from_price(sp_current ** 2)
    # Store tick as unsigned int24 (matching original encoding)
    us["currentTick"] = str(new_tick if new_tick >= 0 else new_tick + INT24_MOD)
    us["liquidity"] = str(int(L))

    amount_out_int = int(amount_out.to_integral(rounding=ROUND_DOWN))
    return amount_out_int, updated_pool, zero_for_one


# ──────────────────────────────────────────────
#  Uniswap V3 public API
# ──────────────────────────────────────────────

def get_v3_amount_out(pool: dict, input_token: str, amount_in: int) -> int:
    """
    Compute output amount for a Uniswap V3 swap.

    Args:
        pool: A V3 pool dict (element from filtered-v3pools.json).
        input_token: Address of the input token (must match token0 or token1).
        amount_in: Input amount in raw token units (int).

    Returns:
        Output amount in raw token units (int).
    """
    amount_out, _, _ = _v3_swap(pool, input_token, amount_in)
    return amount_out


def get_v3_amount_out_with_pool_state(pool: dict, input_token: str, amount_in: int) -> tuple:
    """
    Compute output amount and return updated pool state for a V3 swap.

    Returns:
        (amount_out, updated_pool) where updated_pool is a deep copy with
        updated sqrtPriceX96, currentTick, and liquidity.
    """
    amount_out, updated_pool, _ = _v3_swap(pool, input_token, amount_in)
    return amount_out, updated_pool
