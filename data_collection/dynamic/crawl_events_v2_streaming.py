from web3 import Web3
from eth_utils import to_checksum_address
import json
import datetime
import pandas as pd
import time
import random
import argparse
import os
import glob
from multiprocessing import Process
import traceback
import re

# ----------- 1. 配置与初始化 -----------
w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:4291'))

# 用于收集所有事件数据
all_events_data = []

# Uniswap V2事件签名（Keccak256哈希的topic0）
swap_event_signature = '0x' + Web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
mint_event_signature = '0x' + Web3.keccak(text="Mint(address,uint256,uint256)").hex()
sync_event_signature = '0x' + Web3.keccak(text="Sync(uint112,uint112)").hex()
burn_event_signature = '0x' + Web3.keccak(text="Burn(address,uint256,uint256,address)").hex()
# V3 Initialize事件签名（用于同时监控V3池子的初始化）
initialize_event_signature = '0x' + Web3.keccak(text="Initialize(uint160,int24)").hex()
print(swap_event_signature)
print(f"Initialize signature: {initialize_event_signature}")
# swap_event_signature = "0xd78ad95fa46c994b6551d0da85fc275fe6131c3dfccaa30cba4a5ca6b39fdc67"
# mint_event_signature = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
# burn_event_signature = "0x5c69ee801b4475aa428db9c4c6a7c04c77a7e1dbf4b9f1627a30c7e9d5bb2d17"

# ----------- 2. 事件解析函数 -----------

def _normalize_data_field(raw: object) -> str:
    """Return hex string without 0x prefix, padded to multiple of 64 chars."""
    if isinstance(raw, bytes):
        hex_data = raw.hex()
    else:
        hex_data = str(raw)
        if hex_data.startswith("0x"):
            hex_data = hex_data[2:]
    # pad to 64-char chunks (uint256 words)
    if len(hex_data) % 64 != 0:
        hex_data = hex_data.ljust(((len(hex_data) + 63) // 64) * 64, "0")
    return hex_data


def parse_swap_event(log):
    """解析Swap事件"""
    try:
        # Swap(address sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address to)
        # topics[1] = sender, topics[2] = to
        # topic是32字节(64个hex字符)，地址是20字节(40个hex字符)，右对齐
        # log['topics'][1].hex() 返回 "0x" + 64个字符，取最后40个字符作为地址
        sender = to_checksum_address('0x' + log['topics'][1].hex()[-40:])
        to = to_checksum_address('0x' + log['topics'][2].hex()[-40:])
        
        # 解析data中的数值 (4个uint256)
        hex_data = _normalize_data_field(log['data'])
        amount0_in = int(hex_data[0:64], 16)
        amount1_in = int(hex_data[64:128], 16)
        amount0_out = int(hex_data[128:192], 16)
        amount1_out = int(hex_data[192:256], 16)
        
        return {
            'event_type': 'Swap',
            'sender': sender,
            'to': to,
            'amount0_in': amount0_in,
            'amount1_in': amount1_in,
            'amount0_out': amount0_out,
            'amount1_out': amount1_out,
            'net_amount0': amount0_in - amount0_out,  # 正数表示token0流入pool
            'net_amount1': amount1_in - amount1_out,  # 正数表示token1流入pool
            'trade_direction': 'token0_to_token1' if amount0_in > 0 else 'token1_to_token0'
        }
    except Exception as e:
        print(f"解析Swap事件出错: {e}")
        return {'event_type': 'Swap', 'error': str(e)}

def parse_mint_event(log):
    """解析Mint事件（添加流动性）"""
    try:
        # Mint(address sender, uint amount0, uint amount1)
        # topics[1] = sender
        sender = to_checksum_address('0x' + log['topics'][1].hex()[-40:]) if len(log['topics']) > 1 else None
        
        # 解析data中的数值 (2个uint256)
        hex_data = _normalize_data_field(log['data'])
        if len(hex_data) < 128:
            return {
                'event_type': 'Mint',
                'sender': sender,
                'amount0': 0,
                'amount1': 0,
                'pool_impact': 'liquidity_added',
                'warning': 'data_too_short'
            }
        amount0 = int(hex_data[0:64], 16)
        amount1 = int(hex_data[64:128], 16)
        
        return {
            'event_type': 'Mint',
            'sender': sender,
            'amount0': amount0,
            'amount1': amount1,
            'pool_impact': 'liquidity_added'
        }
    except Exception as e:
        print(f"解析Mint事件出错: {e}")
        return {'event_type': 'Mint', 'error': str(e)}

def parse_sync_event(log):
    """解析Sync事件（储备量同步）"""
    try:
        # Sync(uint112 reserve0, uint112 reserve1)
        # Sync事件没有indexed参数，所有数据都在data中
        hex_data = _normalize_data_field(log['data'])
        reserve0 = int(hex_data[0:64], 16)
        reserve1 = int(hex_data[64:128], 16)
        
        return {
            'event_type': 'Sync',
            'reserve0': reserve0,
            'reserve1': reserve1,
            'pool_impact': 'reserves_updated'
        }
    except Exception as e:
        print(f"解析Sync事件出错: {e}")
        return {'event_type': 'Sync', 'error': str(e)}

def parse_burn_event(log):
    """解析Burn事件（移除流动性）"""
    try:
        # Burn(address sender, uint amount0, uint amount1, address to)
        # topics[1] = sender, topics[2] = to
        sender = to_checksum_address('0x' + log['topics'][1].hex()[-40:]) if len(log['topics']) > 1 else None
        to = to_checksum_address('0x' + log['topics'][2].hex()[-40:]) if len(log['topics']) > 2 else None
        
        # 解析data中的数值 (2个uint256)
        hex_data = _normalize_data_field(log['data'])
        if len(hex_data) < 128:
            return {
                'event_type': 'Burn',
                'sender': sender,
                'to': to,
                'amount0': 0,
                'amount1': 0,
                'pool_impact': 'liquidity_removed',
                'warning': 'data_too_short'
            }
        amount0 = int(hex_data[0:64], 16)
        amount1 = int(hex_data[64:128], 16)
        
        return {
            'event_type': 'Burn',
            'sender': sender,
            'to': to,
            'amount0': amount0,
            'amount1': amount1,
            'pool_impact': 'liquidity_removed'
        }
    except Exception as e:
        print(f"解析Burn事件出错: {e}")
        return {'event_type': 'Burn', 'error': str(e)}

def parse_initialize_event(log):
    """解析Initialize事件（池子初始化 - V3）"""
    try:
        # Initialize(uint160 sqrtPriceX96, int24 tick)
        # No indexed parameters, all data in data field
        hex_data = _normalize_data_field(log['data'])
        
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
        print(f"解析Initialize事件出错: {e}")
        return {'event_type': 'Initialize', 'error': str(e)}

def get_pool_info(w3, pool_address):
    """获取池子的基本信息"""
    try:
        # Uniswap V2 Pair ABI (简化版)
        pair_abi = [
            {
                "constant": True,
                "inputs": [],
                "name": "token0",
                "outputs": [{"name": "", "type": "address"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "token1", 
                "outputs": [{"name": "", "type": "address"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "getReserves",
                "outputs": [
                    {"name": "_reserve0", "type": "uint112"},
                    {"name": "_reserve1", "type": "uint112"},
                    {"name": "_blockTimestampLast", "type": "uint32"}
                ],
                "type": "function"
            }
        ]
        
        contract = w3.eth.contract(address=to_checksum_address(pool_address), abi=pair_abi)
        token0 = contract.functions.token0().call()
        token1 = contract.functions.token1().call()
        reserves = contract.functions.getReserves().call()
        
        return {
            'token0': token0,
            'token1': token1,
            'reserve0': reserves[0],
            'reserve1': reserves[1],
            'last_update': reserves[2]
        }
    except Exception as e:
        print(f"获取池子信息出错 {pool_address}: {e}")
        return None

# ----------- 3. 事件监听和处理主函数 -----------

def handle_new_block(block_number, w3, t_received_override=None, logs_override=None, fast_mode=False):
    """Returns (T_block, T_update) in seconds, or (None, None) if skipped.
    When t_received_override is set (e.g. from combined mode), T_block is not measured (returns None).
    When logs_override is set, skip get_logs and use provided logs (for combined single-fetch).
    When fast_mode=True, skip per-event sleep for lower latency."""
    # Check if block events file already exists (使用更健壮的检查)
    existing_files = glob.glob(f"events_v2_new/uniswap_events_block_{block_number}_*.json")
    if existing_files:
        print(f"⏭️  Skipping block {block_number}: File already exists ({os.path.basename(existing_files[0])})")
        return (None, None)
    
    # 使用锁文件防止多进程重复处理
    lock_file = f"events_v2_new/.lock_block_{block_number}.tmp"
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
    
    try:
        v2_signatures = {swap_event_signature, mint_event_signature, sync_event_signature, burn_event_signature, initialize_event_signature}
        if logs_override is not None:
            def _t0(lg):
                t = lg['topics'][0]
                s = t.hex() if hasattr(t, 'hex') else str(t)
                return s if s.startswith('0x') else '0x' + s
            logs = [lg for lg in logs_override if _t0(lg) in v2_signatures]
        else:
            filter_params = {
                'fromBlock': hex(block_number),
                'toBlock': hex(block_number),
                'topics': [[swap_event_signature, mint_event_signature, sync_event_signature, burn_event_signature, initialize_event_signature]]
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
            print(f"找到 {len(logs)} 个相关事件")

        block_events = []  # 当前区块的事件数据

        for log in logs:
            try:
                topic0 = "0x" + log['topics'][0].hex()
                pair_address = to_checksum_address(log['address'])
                if topic0 == swap_event_signature:
                    parsed_event = parse_swap_event(log)
                elif topic0 == mint_event_signature:
                    parsed_event = parse_mint_event(log)
                elif topic0 == sync_event_signature:
                    parsed_event = parse_sync_event(log)
                elif topic0 == burn_event_signature:
                    parsed_event = parse_burn_event(log)
                elif topic0 == initialize_event_signature:
                    parsed_event = parse_initialize_event(log)
                else:
                    continue  # 跳过未识别的事件
                
                # 获取池子信息
                # pool_info = get_pool_info(w3, pair_address)
                
                # 收集完整的事件数据
                event_data = {
                    'block_number': block_number,
                    'pair_address': pair_address,
                    'transaction_hash': log['transactionHash'].hex(),
                    'log_index': log['logIndex'],
                    'transaction_index': log['transactionIndex'],
                    'parsed_event': parsed_event,
                    # 'pool_info': pool_info,
                    'raw_topics': [topic.hex() for topic in log['topics']],
                    'raw_data': log['data'].hex() if hasattr(log['data'], 'hex') else log['data'],
                    'timestamp': datetime.datetime.now().isoformat()
                }
                all_events_data.append(event_data)
                block_events.append(event_data)
                
                # 打印详细信息
                # print(f"[区块 {block_number}] 事件类型: {parsed_event.get('event_type', 'Unknown')}")
                # print(f"池子地址: {pair_address}")
                # print(f"交易哈希: {log['transactionHash'].hex()}")
                
                # if parsed_event:
                #     if parsed_event['event_type'] == 'Swap':
                #         print(f"交易方向: {parsed_event.get('trade_direction', 'Unknown')}")
                #         print(f"Token0净变化: {parsed_event.get('net_amount0', 0)}")
                #         print(f"Token1净变化: {parsed_event.get('net_amount1', 0)}")
                #     elif parsed_event['event_type'] in ['Mint', 'Burn']:
                #         print(f"池子影响: {parsed_event.get('pool_impact', 'Unknown')}")
                #         print(f"Token0数量: {parsed_event.get('amount0', 0)}")
                #         print(f"Token1数量: {parsed_event.get('amount1', 0)}")
                #     elif parsed_event['event_type'] == 'Sync':
                #         print(f"池子影响: {parsed_event.get('pool_impact', 'Unknown')}")
                #         print(f"新储备量 - Token0: {parsed_event.get('reserve0', 0)}")
                #         print(f"新储备量 - Token1: {parsed_event.get('reserve1', 0)}")
                
                # if pool_info:
                #     print(f"当前储备 - Token0: {pool_info['reserve0']}, Token1: {pool_info['reserve1']}")
                
                # print("-" * 50)
                
                if not fast_mode:
                    sleep_time = random.uniform(0.05, 0.2)
                    time.sleep(sleep_time)

            except Exception as e:
                print(f"处理log时出错: {e}")

        # 每个区块处理完后立即写入数据
        if block_events:
            # 在写入前再次检查文件是否存在（双重检查，防止竞态条件）
            existing_files = glob.glob(f"events_v2_new/uniswap_events_block_{block_number}_*.json")
            if existing_files:
                print(f"⏭️  Skipping block {block_number}: File already exists during write ({os.path.basename(existing_files[0])})")
                return (None, None)  # 锁文件会在 finally 块中清理
            
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"events_v2_new/uniswap_events_block_{block_number}_{timestamp}.json"
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
                print(f"✅ 区块 {block_number} 数据已保存到: {filename} (找到 {len(block_events)} 个事件)")
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
            
            # 更新总体进度文件
            # update_summary_file(block_number, len(block_events))
            
            # 只在有事件时才调用后续处理
            try:
                post_update_processing(block_number)
            except Exception as e:
                print(f"调用后续处理函数时出错: {e}")
            return (t_block, t_update)
        else:
            # 即使没有事件也记录一下，避免静默跳过
            t_end = time.time()
            t_update = t_end - t_received
            print(f"ℹ️  区块 {block_number} 没有找到相关事件")
            if t_block is not None:
                print(f"   T_block={t_block:.3f}s (block mined→received), T_update={t_update:.3f}s (received→state updated)")
            else:
                print(f"   T_update={t_update:.3f}s (received→state updated)")
            return (t_block, t_update)

    except Exception as e:
        print(f"处理区块 {block_number} 时出错: {e}")
        traceback.print_exc()
        return (None, None)
    finally:
        # 清理锁文件（只有在成功创建时才清理）
        if lock_created and os.path.exists(lock_file):
            try:
                os.remove(lock_file)
            except Exception as e:
                print(f"清理锁文件失败 {lock_file}: {e}")

# ----------- 4. 示例后处理函数（可以为空实现） -----------

def post_update_processing(block_number):
    # 可以自定义：例如保存日志、通知、其他业务操作
    pass

# ----------- 5. 主循环调用 ------------

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
            t_block, t_update = handle_new_block(blk, w3)
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
        print(f"\n=== Latency Summary (V2) ===")
        print(f"T_block (block mined→received): avg={sum(t_block_list)/len(t_block_list):.3f}s, min={min(t_block_list):.3f}s, max={max(t_block_list):.3f}s, n={len(t_block_list)}")
    if t_update_list:
        print(f"T_update (received→state updated): avg={sum(t_update_list)/len(t_update_list):.3f}s, min={min(t_update_list):.3f}s, max={max(t_update_list):.3f}s, n={len(t_update_list)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--stream_blocks', type=int, default=5, help='流式处理区块数')
    parser.add_argument('--poll_interval', type=float, default=1.0, help='轮询最新区块间隔(秒)')
    args = parser.parse_args()

    os.makedirs("events_v2_new", exist_ok=True)

    print(f"Start time: {time.time()}")
    stream_latest_blocks(w3, max_blocks=args.stream_blocks, poll_interval=args.poll_interval)
    print(f"End time: {time.time()}")
    print(f"📁 流式区块数据已保存为独立的JSON文件")