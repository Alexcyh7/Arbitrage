from web3 import Web3
from eth_utils import to_checksum_address
import json
import datetime
import time
import random
import argparse
import os
import glob
from multiprocessing import Process
import traceback

# ----------- 1. 配置与初始化 -----------
eth_node_url = 'http://127.0.0.1:4291'

# w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:4291'))

# 用于收集所有事件数据
all_events_data = []

# Uniswap V3事件签名（Keccak256哈希的topic0）
swap_v3_signature = '0x' + Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
mint_v3_signature = '0x' + Web3.keccak(text="Mint(address,address,int24,int24,uint128,uint256,uint256)").hex()
burn_v3_signature = '0x' + Web3.keccak(text="Burn(address,int24,int24,uint128,uint256,uint256)").hex()
initialize_v3_signature = '0x' + Web3.keccak(text="Initialize(uint160,int24)").hex()
collect_v3_signature = '0x' + Web3.keccak(text="Collect(address,address,int24,int24,uint128,uint128)").hex()

print(f"V3 Swap signature: {swap_v3_signature} (len: {len(swap_v3_signature)})")
print(f"V3 Mint signature: {mint_v3_signature} (len: {len(mint_v3_signature)})")
print(f"V3 Burn signature: {burn_v3_signature} (len: {len(burn_v3_signature)})")
print(f"V3 Initialize signature: {initialize_v3_signature} (len: {len(initialize_v3_signature)})")
print(f"V3 Collect signature: {collect_v3_signature} (len: {len(collect_v3_signature)})")

# ----------- 2. 事件解析函数 -----------

def parse_swap_v3_event(log):
    """解析Uniswap V3 Swap事件"""
    try:
        # Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
        # topics[1] = sender, topics[2] = recipient
        # Handle both HexBytes and string topics
        topic1 = log['topics'][1].hex() if hasattr(log['topics'][1], 'hex') else log['topics'][1]
        topic2 = log['topics'][2].hex() if hasattr(log['topics'][2], 'hex') else log['topics'][2]
        sender = to_checksum_address('0x' + topic1[-40:])
        recipient = to_checksum_address('0x' + topic2[-40:])
        
        # 解析data中的数值
        data = log['data']
        
        # Convert to hex string if it's bytes
        if isinstance(data, str):
            hex_data = data
            if hex_data.startswith('0x'):
                hex_data = hex_data[2:]
        else:
            hex_data = data.hex()
        
        # 验证数据长度
        expected_length = 320  # 5 * 32 bytes = 160 bytes = 320 hex chars
        if len(hex_data) != expected_length:
            print(f"⚠️  Swap事件数据长度异常: 期望 {expected_length}, 实际 {len(hex_data)}")
            print(f"   Pool: {log['address']}, TxHash: {log['transactionHash'].hex() if hasattr(log['transactionHash'], 'hex') else log['transactionHash']}")
        
        # Parse the data: 5 * 32 bytes = 160 bytes (320 chars)
        # amount0 (int256): bytes 0-31 (chars 0-63)
        # amount1 (int256): bytes 32-63 (chars 64-127)  
        # sqrtPriceX96 (uint160): bytes 64-83 (chars 128-167, but padded to 64 chars)
        # liquidity (uint128): bytes 96-111 (chars 192-223, but padded to 64 chars)
        # tick (int24): bytes 128-131 (chars 256-263, but padded to 64 chars)
        
        amount0 = int(hex_data[0:64], 16)
        amount1 = int(hex_data[64:128], 16)
        sqrtPriceX96 = int(hex_data[128:192], 16)
        liquidity = int(hex_data[192:256], 16)
        # For tick (int24), only use the last 3 bytes (6 hex chars) of the 32-byte field
        tick = int(hex_data[-6:], 16)
        
        # Convert to signed integers for amounts and tick
        if amount0 >= 2**255:
            amount0 -= 2**256
        if amount1 >= 2**255:
            amount1 -= 2**256
        # Convert int24 tick to signed
        if tick >= 2**23:
            tick = tick - 2**24
            
        return {
            'event_type': 'Swap',
            'sender': sender,
            'recipient': recipient,
            'amount0': amount0,
            'amount1': amount1,
            'sqrtPriceX96': sqrtPriceX96,
            'liquidity': liquidity,
            'tick': tick,
            'trade_direction': 'token0_to_token1' if amount0 > 0 else 'token1_to_token0'
        }
    except Exception as e:
        print(f"❌ 解析V3 Swap事件出错: {e}")
        print(f"   Pool: {log.get('address', 'unknown')}")
        print(f"   TxHash: {log.get('transactionHash', 'unknown')}")
        return {'event_type': 'Swap', 'error': str(e)}

def parse_mint_v3_event(log):
    """解析Uniswap V3 Mint事件"""
    try:
        # Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
        # topics[1] = owner, topics[2] = tickLower, topics[3] = tickUpper
        topic1 = log['topics'][1].hex() if hasattr(log['topics'][1], 'hex') else log['topics'][1]
        topic2 = log['topics'][2].hex() if hasattr(log['topics'][2], 'hex') else log['topics'][2]
        topic3 = log['topics'][3].hex() if hasattr(log['topics'][3], 'hex') else log['topics'][3]
        owner = to_checksum_address('0x' + topic1[-40:])
        tick_lower = int(topic2, 16)
        tick_upper = int(topic3, 16)
        
        # Convert tick values to signed integers
        if tick_lower >= 2**23:
            tick_lower -= 2**24
        if tick_upper >= 2**23:
            tick_upper -= 2**24
        
        # 解析data中的数值
        data = log['data']
        
        if isinstance(data, str):
            hex_data = data
            if hex_data.startswith('0x'):
                hex_data = hex_data[2:]
        else:
            hex_data = data.hex()
        
        # Parse: sender (32 bytes) + amount (16 bytes, padded to 32) + amount0 (32 bytes) + amount1 (32 bytes)
        sender = to_checksum_address('0x' + hex_data[24:64])  # Last 20 bytes of first 32
        amount = int(hex_data[64:128], 16)  # uint128 liquidity amount
        amount0 = int(hex_data[128:192], 16)  # uint256
        amount1 = int(hex_data[192:256], 16)  # uint256
        
        return {
            'event_type': 'Mint',
            'sender': sender,
            'owner': owner,
            'tick_lower': tick_lower,
            'tick_upper': tick_upper,
            'amount': amount,
            'amount0': amount0,
            'amount1': amount1,
            'pool_impact': 'liquidity_added'
        }
    except Exception as e:
        print(f"解析V3 Mint事件出错: {e}")
        return {'event_type': 'Mint', 'error': str(e)}

def parse_burn_v3_event(log):
    """解析Uniswap V3 Burn事件"""
    try:
        # Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
        topic1 = log['topics'][1].hex() if hasattr(log['topics'][1], 'hex') else log['topics'][1]
        topic2 = log['topics'][2].hex() if hasattr(log['topics'][2], 'hex') else log['topics'][2]
        topic3 = log['topics'][3].hex() if hasattr(log['topics'][3], 'hex') else log['topics'][3]
        owner = to_checksum_address('0x' + topic1[-40:])
        tick_lower = int(topic2, 16)
        tick_upper = int(topic3, 16)
        
        # Convert tick values to signed integers
        if tick_lower >= 2**23:
            tick_lower -= 2**24
        if tick_upper >= 2**23:
            tick_upper -= 2**24
        
        # 解析data中的数值
        data = log['data']
        
        if isinstance(data, str):
            hex_data = data
            if hex_data.startswith('0x'):
                hex_data = hex_data[2:]
        else:
            hex_data = data.hex()
        
        # Parse: amount (16 bytes, padded to 32) + amount0 (32 bytes) + amount1 (32 bytes)
        amount = int(hex_data[0:64], 16)  # uint128 liquidity amount
        amount0 = int(hex_data[64:128], 16)  # uint256
        amount1 = int(hex_data[128:192], 16)  # uint256
        
        return {
            'event_type': 'Burn',
            'owner': owner,
            'tick_lower': tick_lower,
            'tick_upper': tick_upper,
            'amount': amount,
            'amount0': amount0,
            'amount1': amount1,
            'pool_impact': 'liquidity_removed'
        }
    except Exception as e:
        print(f"解析V3 Burn事件出错: {e}")
        return {'event_type': 'Burn', 'error': str(e)}

def parse_initialize_v3_event(log):
    """解析Uniswap V3 Initialize事件"""
    try:
        # Initialize(uint160 sqrtPriceX96, int24 tick)
        # No indexed parameters, all data in data field
        data = log['data']
        
        if isinstance(data, str):
            hex_data = data
            if hex_data.startswith('0x'):
                hex_data = hex_data[2:]
        else:
            hex_data = data.hex()
        
        # Parse: sqrtPriceX96 (20 bytes, padded to 32) + tick (3 bytes, padded to 32)
        sqrtPriceX96 = int(hex_data[0:64], 16)  # uint160
        tick = int(hex_data[64:128], 16)  # int24
        
        # Convert tick to signed integer
        if tick >= 2**23:
            tick -= 2**24
            
        return {
            'event_type': 'Initialize',
            'sqrtPriceX96': sqrtPriceX96,
            'tick': tick,
            'pool_impact': 'pool_initialized'
        }
    except Exception as e:
        print(f"解析V3 Initialize事件出错: {e}")
        return {'event_type': 'Initialize', 'error': str(e)}

# ----------- 3. 事件监听和处理主函数 -----------

def handle_new_block(block_number, eth_node_url=None, t_received_override=None, logs_override=None, fast_mode=False):
    """Returns (T_block, T_update) in seconds, or (None, None) if skipped.
    When t_received_override is set (e.g. from combined mode), T_block is not measured (returns None).
    When logs_override is set, skip get_logs and use provided logs (for combined single-fetch).
    When fast_mode=True, skip per-event sleep for lower latency."""
    # 确保目录存在
    os.makedirs("events_v3_new", exist_ok=True)
    
    # Check if block events file already exists
    existing_files = glob.glob(f"events_v3_new/uniswap_v3_events_block_{block_number}_*.json")
    if existing_files:
        print(f"⏭️  Skipping block {block_number}: File already exists ({os.path.basename(existing_files[0])})")
        return (None, None)
    
    # 使用锁文件防止多进程重复处理
    lock_file = f"events_v3_new/.lock_block_{block_number}.tmp"
    lock_created = False
    try:
        # 尝试创建锁文件（原子操作）
        if os.path.exists(lock_file):
            # 如果锁文件存在，检查是否过期（超过5分钟认为是僵死锁）
            lock_age = time.time() - os.path.getmtime(lock_file)
            if lock_age > 300:  # 5分钟
                os.remove(lock_file)
            else:
                print(f"⏭️  Skipping block {block_number}: Being processed by another process")
                return (None, None)
        
        # 创建锁文件
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))
        lock_created = True
    except (OSError, IOError) as e:
        # 如果创建锁文件失败（可能是另一个进程正在创建），跳过
        print(f"⏭️  Skipping block {block_number}: Lock file creation failed: {e}")
        return (None, None)
    
    w3 = Web3(Web3.HTTPProvider(eth_node_url))
    
    try:
        v3_signatures = {swap_v3_signature, mint_v3_signature, burn_v3_signature, initialize_v3_signature}
        if logs_override is not None:
            def _t0(lg):
                t = lg['topics'][0]
                s = t.hex() if hasattr(t, 'hex') else str(t)
                return s if s.startswith('0x') else '0x' + s
            logs = [lg for lg in logs_override if _t0(lg) in v3_signatures]
        else:
            filter_params = {
                'fromBlock': hex(block_number),
                'toBlock': hex(block_number),
                'topics': [[swap_v3_signature, mint_v3_signature, burn_v3_signature, initialize_v3_signature]]
            }
            logs = w3.eth.get_logs(filter_params)
        if t_received_override is not None:
            t_received = t_received_override  # Combined mode: T_block measured once by caller
            t_block = None
        else:
            t_received = time.time()
            block = w3.eth.get_block(block_number)
            t_block = t_received - block.timestamp  # Block arrival latency: mined -> received
        if logs_override is None:
            print(f"Found {len(logs)} V3 events in block {block_number}")

        block_events = []  # 当前区块的事件数据

        for log in logs:
            event_type_name = "Unknown"  # 在 try 外面初始化，避免 UnboundLocalError
            try:
                topic0 = log['topics'][0].hex() if hasattr(log['topics'][0], 'hex') else log['topics'][0]
                pool_address = to_checksum_address(log['address'])  # 使用导入的函数
                
                # 确保 topic0 有 0x 前缀以便比较
                if not topic0.startswith('0x'):
                    topic0 = '0x' + topic0
                
                # Parse different event types
                if topic0 == swap_v3_signature:
                    event_type_name = "Swap"
                    parsed_event = parse_swap_v3_event(log)
                elif topic0 == mint_v3_signature:
                    event_type_name = "Mint"
                    parsed_event = parse_mint_v3_event(log)
                elif topic0 == burn_v3_signature:
                    event_type_name = "Burn"
                    parsed_event = parse_burn_v3_event(log)
                elif topic0 == initialize_v3_signature:
                    event_type_name = "Initialize"
                    parsed_event = parse_initialize_v3_event(log)
                else:
                    print(f"⚠️  未知事件类型，topic0: {topic0}")
                    continue  # Skip unknown events
                
                # 收集完整的事件数据
                event_data = {
                    'block_number': block_number,
                    'pool_address': pool_address,
                    'transaction_hash': log['transactionHash'].hex() if hasattr(log['transactionHash'], 'hex') else log['transactionHash'],
                    'log_index': log['logIndex'],
                    'transaction_index': log['transactionIndex'],
                    'parsed_event': parsed_event,
                    'raw_topics': [topic.hex() if hasattr(topic, 'hex') else topic for topic in log['topics']],
                    'raw_data': log['data'].hex() if hasattr(log['data'], 'hex') else log['data'],
                    'timestamp': datetime.datetime.now().isoformat()
                }
                all_events_data.append(event_data)
                block_events.append(event_data)
                
                if not fast_mode:
                    sleep_time = random.uniform(0.1, 0.3)
                    time.sleep(sleep_time)

            except Exception as e:
                print(f"❌ 处理V3 {event_type_name} log时出错: {e}")
                print(f"   Block: {block_number}, Pool: {log.get('address', 'unknown')}")

        # 每个区块处理完后立即写入数据
        if block_events:
            # 在写入前再次检查文件是否存在（双重检查，防止竞态条件）
            existing_files = glob.glob(f"events_v3_new/uniswap_v3_events_block_{block_number}_*.json")
            if existing_files:
                print(f"⏭️  Skipping block {block_number}: File already exists during write ({os.path.basename(existing_files[0])})")
                return (None, None)  # 锁文件会在 finally 块中清理
            
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"events_v3_new/uniswap_v3_events_block_{block_number}_{timestamp}.json"
            temp_filename = f"{filename}.tmp"
            
            # 先写入临时文件，然后原子性地重命名
            try:
                with open(temp_filename, 'w', encoding='utf-8') as f:
                    json.dump({
                        'block_info': {
                            'block_number': block_number,
                            'events_count': len(block_events),
                            'scan_time': datetime.datetime.now().isoformat()
                        },
                        'events': block_events
                    }, f, indent=2, ensure_ascii=False)
                
                # 原子性地重命名（如果文件已存在会失败）
                if os.path.exists(filename):
                    os.remove(temp_filename)
                    print(f"⏭️  Skipping block {block_number}: File created by another process")
                    return (None, None)
                
                os.rename(temp_filename, filename)
                t_end = time.time()
                t_update = t_end - t_received  # State update latency: received -> local state updated
                print(f"✅ V3 Block {block_number} data saved to: {filename} (找到 {len(block_events)} 个事件)")
                if t_block is not None:
                    print(f"   T_block={t_block:.3f}s (block mined→received), T_update={t_update:.3f}s (received→state updated)")
                else:
                    print(f"   T_update={t_update:.3f}s (received→state updated)")
            except (OSError, IOError) as e:
                # 如果写入失败，清理临时文件
                if os.path.exists(temp_filename):
                    try:
                        os.remove(temp_filename)
                    except:
                        pass
                raise e
        else:
            # 即使没有事件也记录一下，避免静默跳过
            t_end = time.time()
            t_update = t_end - t_received
            print(f"ℹ️  V3 区块 {block_number} 没有找到相关事件")
            if t_block is not None:
                print(f"   T_block={t_block:.3f}s (block mined→received), T_update={t_update:.3f}s (received→state updated)")
            else:
                print(f"   T_update={t_update:.3f}s (received→state updated)")
            try:
                post_update_processing(block_number)
            except Exception as e:
                print(f"调用后续处理函数时出错: {e}")
            return (t_block, t_update)
        
        try:
            post_update_processing(block_number)
        except Exception as e:
            print(f"调用后续处理函数时出错: {e}")
        return (t_block, t_update)

    except Exception as e:
        error_msg = str(e)
        print(f"❌ 处理V3区块 {block_number} 时出错: {error_msg}")
        
        # 如果是RPC错误，打印更多调试信息
        if 'code' in error_msg or '-32' in error_msg:
            print(f"   这是一个RPC错误，请检查:")
            print(f"   1. 节点连接是否正常")
            print(f"   2. 区块号是否有效: {block_number}")
            print(f"   3. 过滤器参数是否正确")
        
        # 打印详细的异常信息
        traceback.print_exc()
        return (None, None)
    finally:
        # 清理锁文件（只有在成功创建时才清理）
        if lock_created and os.path.exists(lock_file):
            try:
                os.remove(lock_file)
            except Exception as e:
                print(f"清理锁文件失败 {lock_file}: {e}")

# ----------- 4. 示例后处理函数 -----------

def post_update_processing(block_number):
    print(f"V3 区块 {block_number} 后续处理")

# ----------- 5. 主循环调用 -----------

def stream_latest_blocks(w3, max_blocks=5, poll_interval=2.0):
    """从最新区块开始，流式处理接下来的 max_blocks 个区块"""
    try:
        last_processed = w3.eth.block_number - 1
    except Exception as e:
        print(f"无法获取最新区块号: {e}")
        return

    print(f"Start streaming from latest block: {last_processed + 1}")
    t_block_list = []
    t_update_list = []
    processed = 0

    while processed < max_blocks:
        try:
            latest = w3.eth.block_number
        except Exception as e:
            print(f"获取最新区块号失败: {e}")
            time.sleep(poll_interval)
            continue

        if latest <= last_processed:
            time.sleep(poll_interval)
            continue

        for blk in range(last_processed + 1, latest + 1):
            t_block, t_update = handle_new_block(blk, eth_node_url)
            if t_block is not None:
                t_block_list.append(t_block)
            if t_update is not None:
                t_update_list.append(t_update)
            processed += 1
            last_processed = blk
            if processed >= max_blocks:
                break

    # Summary
    if t_block_list:
        print(f"\n=== Latency Summary (V3) ===")
        print(f"T_block (block mined→received): avg={sum(t_block_list)/len(t_block_list):.3f}s, min={min(t_block_list):.3f}s, max={max(t_block_list):.3f}s, n={len(t_block_list)}")
    if t_update_list:
        print(f"T_update (received→state updated): avg={sum(t_update_list)/len(t_update_list):.3f}s, min={min(t_update_list):.3f}s, max={max(t_update_list):.3f}s, n={len(t_update_list)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--stream_blocks', type=int, default=5, help='流式处理区块数')
    parser.add_argument('--poll_interval', type=float, default=2.0, help='轮询最新区块间隔(秒)')
    args = parser.parse_args()

    w3 = Web3(Web3.HTTPProvider(eth_node_url))

    print(f"Start time: {time.time()}")
    stream_latest_blocks(w3, max_blocks=args.stream_blocks, poll_interval=args.poll_interval)
    print(f"End time: {time.time()}")
    print(f"📁 流式V3区块数据已保存为独立的JSON文件")