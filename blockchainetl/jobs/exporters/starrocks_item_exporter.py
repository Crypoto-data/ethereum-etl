# MIT License
#
# Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import collections
import logging
import subprocess
import time
import json

from sqlalchemy import create_engine

from ethereumetl.jobs.exporters.blocks_and_transactions_item_exporter import blocks_and_transactions_item_exporter
from ethereumetl.jobs.exporters.blocks_and_transactions_item_exporter import BLOCK_FIELDS_TO_EXPORT
from ethereumetl.jobs.exporters.blocks_and_transactions_item_exporter import TRANSACTION_FIELDS_TO_EXPORT
from blockchainetl.file_utils import get_file_handle, close_silently
from blockchainetl.exporters import CsvItemExporter, JsonLinesItemExporter


# Two methods to export block and transaction 
# 1. ethereumetl stream -e block,transaction --start_block xxx --log-file log.txt --provider-uri http://54.251.224.17:8545 --output=http://10.148.0.2:8030
# 2. echo 'xxx' > last_synced_block.txt 
#    ethereumetl stream -e block,transaction --log-file log.txt --provider-uri http://54.251.224.17:8545 --output=http://10.148.0.2:8030

class StarRocksItemExporter:

    def __init__(self, connection_url, table_mapping, field_mapping=None):
        self.connection_url = connection_url
        self.column_separator = ","
        self.database = "eth"
        self.user = "root:crypto@123"
        self.table_mapping = table_mapping
        self.field_mapping = field_mapping
        self.filename_mapping = {}
        self.file_mapping = {}
        self.exporter_mapping = {}
        self.columns_mapping = {"block" : "columns:number,hash,parent_hash,nonce,sha3_uncles,logs_bloom,transactions_root,state_root,receipts_root,miner,difficulty,total_difficulty,size,extra_data,gas_limit,gas_used,block_timestamp,transaction_count,base_fee_per_gas,block_time=from_unixtime(block_timestamp-28800)",
                                "transaction" : "columns:hash,nonce,block_hash,block_number,transaction_index,from_address,to_address,value,gas,gas_price,input,block_timestamp,max_fee_per_gas,max_priority_fee_per_gas,transaction_type,block_time=from_unixtime(block_timestamp-28800)"}
        self.block_counter = 0
    
    def get_label(self, table):
        t = time.time().__str__().replace(".", "_")
        return "_".join([self.database, table, t])

    def load(self, item_type):
        table = self.table_mapping[item_type]
        filename = self.filename_mapping[item_type]
        label = self.get_label(table)
        columns = self.columns_mapping[item_type]

        url = "%s/api/%s/%s/_stream_load" % (self.connection_url, self.database, table)
        params = [
            "curl",
            "--location-trusted",
            "-XPUT",
            "-u",
            "%s" % self.user,
            "-T",
            "%s" % filename,
            "-H",
            "Expect: 100-continue",
            "-H",
            "label:%s" % label,
            "-H",
            "column_separator:,",
            "-H",
            "%s" % columns,
            "-H",
            "max_filter_ratio:0.5"
        ]

        params.append(url)
        stream_load_sql = " ".join(param for param in params)
        cmdRes = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=1800)
        res = json.loads(cmdRes.stdout)
        if (res["Status"] != "Success" and res["Status"] != "Publish Timeout"
            and res["Message"] != "all partitions have no load data"):
            logging.info(str(res))
            exit(0)

    def open(self):
        for item_type, table in self.table_mapping.items():
            filename = table + "_" + str(round(time.time() * 1000))
            self.filename_mapping[item_type] = filename
            file = get_file_handle(filename, binary=True)
            self.file_mapping[item_type] = file
            item_exporter = CsvItemExporter(file, fields_to_export=self.field_mapping[item_type])
            self.exporter_mapping[item_type] = item_exporter

    def export_items(self, items):
        for item in items:
            item_type = item.get("type")
            if item_type == "block":
                self.block_counter += 1
            exporter = self.exporter_mapping.get(item_type)
            exporter.export_item(item)
        if self.block_counter >= 1:
            for item_type, table in self.table_mapping.items():
                file = self.file_mapping[item_type]
                close_silently(file)
                self.load(item_type)

                # open new file
                filename = table + "_" + str(round(time.time() * 1000))
                self.filename_mapping[item_type] = filename
                file = get_file_handle(filename, binary=True)
                self.file_mapping[item_type] = file
                item_exporter = CsvItemExporter(file, fields_to_export=self.field_mapping[item_type])
                self.exporter_mapping[item_type] = item_exporter
            self.block_counter = 0

    def close(self):
        if self.block_counter == 0:
            return
        for item_type in self.table_mapping.keys():
            file = self.file_mapping[item_type]
            close_silently(file)
            self.load(item_type)
