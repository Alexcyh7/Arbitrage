import copy
import math
import re
from decimal import Decimal, getcontext, ROUND_DOWN

getcontext().prec = 256

Q96 = Decimal(2) ** 96


# ──────────────────────────────────────────────
#  Hex / signed-integer helpers
# ──────────────────────────────────────────────

def _hex_to_uint(h: str) -> int:
    """Convert a hex string (0x...) to unsigned int."""
    return int(h, 16)


def _hex_to_int(h: str) -> int:
    """Convert a hex string to signed int (two's complement based on byte length)."""
    raw = int(h, 16)
    hex_digits = h[2:] if h.startswith("0x") or h.startswith("0X") else h
    # Pad to even number of hex digits
    if len(hex_digits) % 2 != 0:
        hex_digits = "0" + hex_digits
    byte_len = len(hex_digits) // 2
    # Check sign bit
    if raw >= (1 << (byte_len * 8 - 1)):
        raw -= 1 << (byte_len * 8)
    return raw


def _int_to_hex(val: int) -> str:
    """Convert a signed or unsigned int back to hex string."""
    if val >= 0:
        return hex(val)
    # Two's complement: find minimal byte length
    byte_len = (val.bit_length() + 8) // 8  # enough bytes for magnitude + sign
    unsigned = val + (1 << (byte_len * 8))
    return hex(unsigned)


# ──────────────────────────────────────────────
#  Uniswap V2
# ──────────────────────────────────────────────

def _v2_swap(pool: dict, input_token: str, amount_in: int):
    """
    Core V2 constant-product swap.
    Returns (amount_out, zero_for_one).

    Pool format (snapshot):
      component.tokens: [token0, token1]
      component.static_attributes.fee: hex (e.g. "0x1e" = 30, meaning 30/10000 = 0.3%)
      state.attributes.reserve0 / reserve1: hex
    """
    tokens = pool["component"]["tokens"]
    token0 = tokens[0].lower()
    token1 = tokens[1].lower()
    input_lower = input_token.lower()

    attrs = pool["state"]["attributes"]
    reserve0 = _hex_to_uint(attrs["reserve0"])
    reserve1 = _hex_to_uint(attrs["reserve1"])

    if input_lower == token0:
        reserve_in, reserve_out = reserve0, reserve1
        zero_for_one = True
    elif input_lower == token1:
        reserve_in, reserve_out = reserve1, reserve0
        zero_for_one = False
    else:
        raise ValueError(f"input_token {input_token} not in pool")

    fee = _hex_to_uint(pool["component"]["static_attributes"]["fee"])
    # fee is in units of 1/10000 (e.g. 30 = 0.3%)
    amount_in_with_fee = amount_in * (10_000 - fee)
    numerator = amount_in_with_fee * reserve_out
    denominator = reserve_in * 10_000 + amount_in_with_fee
    amount_out = numerator // denominator

    return amount_out, zero_for_one


def get_v2_amount_out(pool: dict, input_token: str, amount_in: int) -> int:
    """
    Compute output amount for a Uniswap V2 swap.

    Args:
        pool: A V2 pool dict from the snapshot JSON.
        input_token: Address of the input token.
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
        (amount_out, updated_pool) with adjusted reserves and balances.
    """
    amount_out, zero_for_one = _v2_swap(pool, input_token, amount_in)

    updated_pool = copy.deepcopy(pool)
    attrs = updated_pool["state"]["attributes"]
    balances = updated_pool["state"]["balances"]
    tokens = updated_pool["component"]["tokens"]

    r0 = _hex_to_uint(attrs["reserve0"])
    r1 = _hex_to_uint(attrs["reserve1"])

    if zero_for_one:
        r0 += amount_in
        r1 -= amount_out
    else:
        r1 += amount_in
        r0 -= amount_out

    attrs["reserve0"] = hex(r0)
    attrs["reserve1"] = hex(r1)
    balances[tokens[0]] = hex(r0)
    balances[tokens[1]] = hex(r1)

    return amount_out, updated_pool


# ──────────────────────────────────────────────
#  Uniswap V3 helpers
# ──────────────────────────────────────────────

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


def _parse_v3_ticks(attrs: dict):
    """
    Parse tick data from state.attributes.
    Keys like "ticks/{idx}/net-liquidity" -> list of (tick_idx, net_liquidity).
    net-liquidity values are signed hex.
    """
    tick_pattern = re.compile(r'^ticks/(-?\d+)/net-liquidity$')
    ticks = []
    for key, val in attrs.items():
        m = tick_pattern.match(key)
        if m:
            tick_idx = int(m.group(1))
            net_liq = _hex_to_int(val)
            ticks.append((tick_idx, net_liq))
    return ticks


# ──────────────────────────────────────────────
#  Uniswap V3 swap engine
# ──────────────────────────────────────────────

def _v3_swap(pool: dict, input_token: str, amount_in: int):
    """
    Core V3 tick-based swap simulation.
    Returns: (amount_out, updated_pool, zero_for_one)

    Pool format (snapshot):
      component.tokens: [token0, token1]
      component.static_attributes.fee: hex (e.g. "0x0bb8" = 3000, meaning 3000/1000000)
      state.attributes.sqrt_price_x96: hex
      state.attributes.tick: hex (signed)
      state.attributes.liquidity: hex
      state.attributes["ticks/{idx}/net-liquidity"]: hex (signed)
    """
    tokens = pool["component"]["tokens"]
    token0 = tokens[0].lower()
    token1 = tokens[1].lower()
    input_lower = input_token.lower()

    if input_lower == token0:
        zero_for_one = True
    elif input_lower == token1:
        zero_for_one = False
    else:
        raise ValueError(f"input_token {input_token} not in pool")

    attrs = pool["state"]["attributes"]
    fee = _hex_to_uint(pool["component"]["static_attributes"]["fee"])
    fee_rate = Decimal(fee) / Decimal(1_000_000)

    sp_current = _sqrt_price_x96_to_decimal(_hex_to_uint(attrs["sqrt_price_x96"]))
    if sp_current <= Decimal('1e-18'):
        sp_current = Decimal('1e-18')

    L = Decimal(_hex_to_uint(attrs["liquidity"]))
    current_tick = _hex_to_int(attrs["tick"])

    all_ticks = _parse_v3_ticks(attrs)

    amount_remaining = Decimal(amount_in)
    amount_out = Decimal(0)
    EPSILON = Decimal('1e-18')

    if zero_for_one:
        # token0 -> token1: price decreases, walk ticks downward
        ticks = sorted(
            [(idx, nl) for idx, nl in all_ticks if idx <= current_tick],
            key=lambda x: -x[0]
        )

        for tick_idx, net_liq in ticks:
            if amount_remaining <= 0:
                break

            sp_target = _sqrt_price_from_tick(tick_idx)
            if sp_target >= sp_current:
                continue

            if L < EPSILON:
                break
            dx_net = L * (sp_current - sp_target) / (sp_current * sp_target)
            dx_gross = dx_net / (Decimal(1) - fee_rate)

            if amount_remaining >= dx_gross:
                dy = L * (sp_current - sp_target)
                amount_out += dy
                amount_remaining -= dx_gross
                sp_current = sp_target
                L -= Decimal(net_liq)
            else:
                dx_net_avail = amount_remaining * (Decimal(1) - fee_rate)
                sp_new = (L * sp_current) / (L + dx_net_avail * sp_current)
                sp_new = sp_new.quantize(Decimal('1e-18'), rounding=ROUND_DOWN)
                amount_out += L * (sp_current - sp_new)
                amount_remaining = Decimal(0)
                sp_current = sp_new

        if amount_remaining > 0 and L >= EPSILON:
            dx_net_avail = amount_remaining * (Decimal(1) - fee_rate)
            sp_new = (L * sp_current) / (L + dx_net_avail * sp_current)
            sp_new = sp_new.quantize(Decimal('1e-18'), rounding=ROUND_DOWN)
            amount_out += L * (sp_current - sp_new)
            sp_current = sp_new

    else:
        # token1 -> token0: price increases, walk ticks upward
        ticks = sorted(
            [(idx, nl) for idx, nl in all_ticks if idx > current_tick],
            key=lambda x: x[0]
        )

        for tick_idx, net_liq in ticks:
            if amount_remaining <= 0:
                break

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
                L += Decimal(net_liq)
            else:
                dy_net_avail = amount_remaining * (Decimal(1) - fee_rate)
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
    ua = updated_pool["state"]["attributes"]
    ua["sqrt_price_x96"] = hex(_decimal_to_sqrt_price_x96(sp_current))
    new_tick = _tick_from_price(sp_current ** 2)
    ua["tick"] = _int_to_hex(new_tick)
    ua["liquidity"] = hex(int(L))

    amount_out_int = int(amount_out.to_integral(rounding=ROUND_DOWN))
    return amount_out_int, updated_pool, zero_for_one


# ──────────────────────────────────────────────
#  Uniswap V3 public API
# ──────────────────────────────────────────────

def get_v3_amount_out(pool: dict, input_token: str, amount_in: int) -> int:
    """
    Compute output amount for a Uniswap V3 swap.

    Args:
        pool: A V3 pool dict from the snapshot JSON.
        input_token: Address of the input token.
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
        (amount_out, updated_pool) with updated sqrt_price_x96, tick, and liquidity.
    """
    amount_out, updated_pool, _ = _v3_swap(pool, input_token, amount_in)
    return amount_out, updated_pool
