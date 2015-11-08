import requests
import json
from time import strptime, time
from calendar import timegm
import sys
from binascii import hexlify
from pycoin.tx import Spendable, tx_utils, TxOut
from pycoin.tx.script import tools
from pycoin.key import Key
from binascii import unhexlify, hexlify
from pycoin_ext import LazySecretExponentDB
from config import config

now = int(time())

def create_spend(utxo):
  sp = Spendable(coin_value   = utxo['amount'] * 10**6,
                 script       = unhexlify(utxo['scriptPubKey']),
                 tx_hash      = unhexlify(utxo['txid'])[::-1],
                 tx_out_index = utxo['vout'])
  return sp

def create_spend_from_tx(tx, index):
  vout = tx['vout'][index]
  utxo = {'amount':       vout['value'],
          'scriptPubKey': vout['scriptPubKey']['hex'],
          'txid': tx['txid'],
          'vout': index}
  return create_spend(utxo)

def do_rq(method, params = []):
  to_json = {'method': method, 'params': params, 'id': 1}
  r = requests.post(config['coinuri'], data = json.dumps(to_json))
  return r.json()['result']


def search_last_tx_data(sps, full_tx = False):
  sp_data = []
  for sp in sps:
    tx = do_rq('getrawtransaction', [hexlify(sp.tx_hash[::-1]).decode('utf-8'), 1])
    for out in tx['vout']:
      if out['scriptPubKey']['hex'][:2] == '6a': # Check for OP_RETURN
        sp_data.append((sp, out['scriptPubKey']))
        if full_tx:
          for vin in tx['vin']:
            old_tx = do_rq('getrawtransaction', [vin['txid'], 1])
            old_sp = create_spend_from_tx(old_tx, vin['vout'])
            last_sp, last_script = search_last_tx_data([old_sp])
            if last_script != None:
              sps.append(last_sp)
  if len(sp_data) > 0: # TODO configure which one to choose
    return sp_data if full_tx else sp_data[0]
  return sps[0], None

def get_wifs(addrs):
  lst = []
  for addr in addrs:
    wif = do_rq('dumpprivkey', [addr])
    if wif == None:
      continue
    lst.append((addr, wif.rstrip()))
  return lst

# TODO
fee_per_kb = 10**5
def estimate_fee(sps, to_addrs):
  #fee_kb = (len(sps) * 119 >> 10) * fee_per_kb
  #print((len(sps),fee_kb), file=sys.stderr)
  return 1 
  return fee_per_kb

def extract_msg(script):
  msg_hex   = unhexlify(script['hex'])
  opcode, msg, _ = tools.get_opcode(msg_hex, 1)
  if 0x51 <= opcode and opcode <= 0x60:
    msg = bytes(chr(opcode - 0x50), 'utf-8')
  return msg

def format_msg(msg):
  op = 'OP_RETURN %s' % msg
  op = tools.compile(op)
  return TxOut(10**4, op)

def create_tx(sp, addrs, msg):
  addrs_wifs = get_wifs(addrs)
  addrs, wifs = zip(*addrs_wifs)
  fee = estimate_fee([sp], addrs[:1])
  tx = tx_utils.create_tx([sp], [addrs[:1][0]], fee=fee, time=now)
  tx.txs_out[0].coin_value -= 2 * 10**4
  tx.txs_out.append(format_msg(msg))
  wifs=[wifs[0]]
  tx.sign(LazySecretExponentDB(wifs, {}, [b'\x34', b'\x44']))
  for idx, tx_out in enumerate(tx.txs_in):
      if not tx.is_signature_ok(idx):
          print('failed to sign spendable for %s' %
                                      tx.unspents[idx].bitcoin_address(),
                                      file=sys.stderr)
  return tx

def get_utxos():
  utxos = do_rq('listunspent', [0])
  addrs = set()
  sps = []
  for i in utxos:
    sp = create_spend(i)
    sps.append(sp)
    addrs.add(i['address'])
  return sps, addrs

def usage():
  print('FIXME TODO', file=sys.stderr)
  exit(-1)

def get_msg():
  if len(sys.argv) != 2:
    usage()
  msg = sys.argv[1]
  msg_b = bytes(msg, ('utf-8'))
  if len(msg_b) > 80 or len(msg_b) == 0:
    usage()
  return hexlify(msg_b).decode('utf-8')

def print_last_msg(last_script):
  last_msg = extract_msg(last_script)
  if last_msg == None:
    print('No msg found.', file=sys.stderr)
  else:
    sys.stdout.buffer.write(last_msg)

def prepare_data():
  sps, addrs = get_utxos()
  if len(sps) > 0:
    sp, last_script = search_last_tx_data(sps)
    return sp, addrs, last_script
  return None, None, None

def auto_put_data(sp, addrs, data):
  tx = create_tx(sp, addrs, data)
  #print(do_rq('decoderawtransaction', [tx.as_hex(True)]), file=sys.stderr)
  return do_rq('sendrawtransaction', [tx.as_hex(True), 1])

if __name__ == '__main__':
  if len(sys.argv) > 3:
    usage()
  sp, addrs, last_script = prepare_data()
  if len(sys.argv) == 1:
    if last_script is not None:
      all_tx = search_last_tx_data([sp], True)
      for _, script in all_tx:
        print_last_msg(script)
    exit(0)
  else:
    if sp != None:
      print(auto_put_data(sp, addrs, get_msg()))
    else:
      print("No spendable output found", file=sys.stderr)
