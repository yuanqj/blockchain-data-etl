import time
import json
import signal
import chardet
import logging
import argparse
import threading
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler
from pythonjsonlogger import jsonlogger
from bitcoinetl.enumeration.chain import Chain
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from bitcoinetl.jobs.export_blocks_job import ExportBlocksJob
from bitcoinetl.jobs.enrich_transactions import EnrichTransactionsJob
from aliyun.log import LogClient, LogItem, PutLogsRequest
from aliyun.log.util import PrefixLoggerAdapter
import aliyun.log.logclient


aliyun.log.logclient.DEFAULT_QUERY_RETRY_COUNT = 1000000


TypeBlock, TypeTransaction = "block", "transaction"

TopicBlock = "block"
TopicTransaction = "transaction"
TopicInput = "input"
TopicOutput = "output"

KeyTimestamp = "_ts_"
KeyBlock = "_bk_"
KeyTransaction = "_tx_"
KeyMetas = [KeyTimestamp, KeyBlock, KeyTransaction]


def _parse_args():
    parser = argparse.ArgumentParser(description='Sync of Xiao Data for Olympics')
    _require_arg = partial(parser.add_argument, required=True)
    _require_arg("--ak-id")
    _require_arg("--ak-secret")
    _require_arg("--endpoint")
    _require_arg("--project")
    _require_arg("--logstore")
    _require_arg("--btc-provider", dest="provider")
    _require_arg("--start-block", dest="bk0", type=int)
    _require_arg("--end-block", dest="bk1", type=int)
    _require_arg("--workers", type=int)

    return parser.parse_known_args()[0]


_args = _parse_args()
_sls_client = LogClient(_args.endpoint, _args.ak_id, _args.ak_secret)
_running = True


def _termsig_handler(sig, frame):
    print(f"termination signal received[{sig}]. exiting...")
    global _running
    _running = False

signal.signal(signal.SIGINT, _termsig_handler)
signal.signal(signal.SIGTERM, _termsig_handler)


def _setup_file_logging(level, filename, max_bytes):
    _log_fields = [
        "asctime",
        "filename",
        "funcName",
        "levelname",
        "lineno",
        "module",
        "message",
        "threadName"
    ]

    root = logging.getLogger()
    root.setLevel(level)
    _format = " ".join("%({0:s})".format(key) for key in _log_fields)
    _handler = RotatingFileHandler(filename, maxBytes=max_bytes, backupCount=1)
    _handler.setFormatter(jsonlogger.JsonFormatter(_format))
    root.addHandler(_handler)

_logger_name = f"blockchain-data-sync-bitcoin-{_args.bk0}-{_args.bk1}"
_setup_file_logging(logging.INFO, f"{_logger_name}.log", 1024**2 * 30)
_logger = logging.getLogger(_logger_name)


def to_str(v):
    if isinstance(v, str):
        return v
    elif v is None:
        return ""
    elif isinstance(v, bytes):
        try:
            res = chardet.detect(v)
            enc = res.get("encoding") if isinstance(res, dict) else "utf8"
            return v.decode(enc)
        except:
            return v.decode("utf8", errors="replace")

    try:
        # cover bool, list, dict, tuple, int, double etc.
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


class SLSExporter(object):
    
    batch_size = 1024

    def __init__(self, client, project, logstore, timestamp) -> None:
        self._client, self._project, self._logstore = client, project, logstore
        self._timestamp, self._events = timestamp, {}

    def export_item(self, item):
        raise NotImplemented()
    
    def process_event(self, topic, event):
        if topic not in self._events:
            self._events[topic] = []
        self._events[topic].append(event)
    
    def flush(self):
        for topic, events in self._events.items():
            for p in range(0, len(events), self.batch_size):
                self._post_events(events[p:p+self.batch_size], topic)
        self._events = {}
    
    def _post_events(self, events, topic):
        items = []
        for e in events:
            kv = [(k, to_str(v)) for k, v in e.items()]
            item = LogItem(timestamp=self._timestamp,  contents=kv)
            items.append(item)

        req = PutLogsRequest(
            project=_args.project,
            logstore=_args.logstore,
            logitems=items,
            topic=topic,
        )
        for _ in range(8191):
            try:
                _sls_client.put_logs(req)
            except:
                time.sleep(3)
            else:
                break

    def open(self): pass

    def close(self): pass


class SLSExporterBlock(SLSExporter):
    def __init__(self, *args, **kwargs) -> None:
        super(SLSExporterBlock, self).__init__(*args, **kwargs)
        self.txs = []

    def export_item(self, item):
        if item["type"] == TypeBlock:
            item.pop("type")
            item[KeyTimestamp] = item.pop("timestamp")
            item[KeyBlock] = item.pop("number")
            self.process_event(TopicBlock, item)
        else:
            self.txs.append(item)


class SLSExporterTransaction(SLSExporter):

    metas_map = {
        KeyTimestamp: "block_timestamp",
        KeyBlock: "block_number",
        KeyTransaction: "index",
    }

    fields_tx = list(metas_map.keys()) + [
        "hash",
        "size",
        "virtual_size",
        "version",
        "lock_time",
        "is_coinbase",
        "input_count",
        "output_count",
        "input_value",
        "output_value",
        "fee",
    ]

    def export_item(self, item):
        if item["type"] != TypeTransaction:
            return
        for k0, k1 in self.metas_map.items():
            item[k0] = item.pop(k1)
        inputs = item["inputs"]
        outputs = item["outputs"]
        is_coinbase = item["is_coinbase"]
        item["is_coinbase"] = "y" if is_coinbase else "n"
        
        tx = {k: item[k] for k in self.fields_tx if k in item}
        self.process_event(TopicTransaction, tx)
        metas = {k: tx[k] for k in KeyMetas}

        for i in inputs:
            i.update(metas)
            i["addresses"] = ",".join(i["addresses"])
            self.process_event(TopicInput, i)
        
        for o in outputs:
            o.update(metas)
            o["addresses"] = ",".join(o["addresses"])
            if is_coinbase:
                o["is_coinbase"] = "y"
            self.process_event(TopicOutput, o)


def export_block(rpc, block_number, logger):
    timestamp = int(time.time())
    while True:
        try:
            exporter = SLSExporterBlock(_sls_client, _args.project, _args.logstore, timestamp)
            job = ExportBlocksJob(
                chain=Chain.BITCOIN,
                bitcoin_rpc = rpc,
                item_exporter=exporter,
                start_block=block_number,
                end_block=block_number,
                export_blocks=True,
                export_transactions=True,
                batch_size=1,
                max_workers=5,
            )
            job.run()

            exporter_tx = SLSExporterTransaction(_sls_client, _args.project, _args.logstore, timestamp)
            job_tx = EnrichTransactionsJob(
                chain=Chain.BITCOIN,
                bitcoin_rpc = rpc,
                item_exporter = exporter_tx,
                transactions_iterable = exporter.txs,
                batch_size=1,
                max_workers=5,
            )
            job_tx.run()
        except:
            logger.error("export block failed: %s", block_number, exc_info=True)
            time.sleep(3)
        else:
            exporter.flush()
            exporter_tx.flush()
            break
    

class Manager(object):
    def __init__(self, start, end) -> None:
        self._bk, self._start, self._end = start, start, end
        self._lock = threading.RLock()

    def next_block(self):
        global _bk
        with self._lock:
            if not _running or self._bk >= self._end:
                return None
            
            self._bk += 1
            return self._bk - 1
    
    def view_block(self):
        return self._bk

_mgr = None


def _sync(worker):
    logger = PrefixLoggerAdapter("", {"worker": str(worker)}, _logger, {})
    rpc = ThreadLocalProxy(lambda: BitcoinRpc(_args.provider))
    while True:
        bk = _mgr.next_block()
        if bk is None:
            break

        print(f"> sync block: #{worker:02d}, {bk:,d}")
        logger.info("sync block starts: %s", bk)
        try:
            export_block(rpc, bk, logger)
        except:
            logger.error("sync block failed: %s", bk, exc_info=True)
            break
        logger.info("sync block completes: %s", bk)
    
    logger.info("sync worker exits")
    print(f"\nworker exits #{worker:02d}")


def _qry_next_block(project, logstore, start_block, end_block):
    resp = _sls_client.get_log_all(
        project=project,
        logstore=logstore,
        query=f"__topic__: block and _bk_>={start_block} and _bk_<{end_block} | select max(_bk_) as bk",
        from_time="-1000d",
        to_time="now",
    )

    bk = start_block
    for batch in resp:
        for item in batch.body:
            try:
                bk = int(item['bk']) + 1
            except:
                pass
    return bk


def main():
    bk0 = _args.bk0
    print(">>> query current status...")
    bk0 = _qry_next_block(_args.project, _args.logstore, _args.bk0, _args.bk1)
    
    if bk0 >= _args.bk1:
        print(f"\n\n>>> exits! already completed, current: {bk0}\n")
        return
    
    msg = f"sync data: [{bk0}, {_args.bk1})"
    _logger.info(msg)
    print(">>>", msg)
    time.sleep(3)

    global _mgr
    _mgr = Manager(bk0, _args.bk1)

    with ThreadPoolExecutor(max_workers=_args.workers) as executor:
        fs = [executor.submit(_sync, _) for _ in range(_args.workers)]
        list(as_completed(fs))
    
    print(f"\n\n>>> exits! next block: {_mgr.view_block()}\n")


if __name__ == '__main__':
    main()
