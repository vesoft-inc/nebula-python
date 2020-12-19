#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import copy
import logging
from threading import RLock, Condition

from nebula2.common.ttypes import HostAddr
from nebula2.storage.ttypes import ErrorCode


class PartInfo(object):
    def __init__(self, part_id, leader: HostAddr, cursor=None):
        self.part_id = part_id
        self.leader = leader
        self.cursor = cursor
        self.has_done = False

    def __repr__(self) -> str:
        return 'PartInfo: part_id: {}, leader: {}, cursor: {}, has_done: {}'\
            .format(self.part_id, self.leader, self.cursor, self.has_done)


class PartManager(object):
    def __init__(self, parts):
        self._lock = RLock()
        self._condition = Condition()
        self._parts = parts
        if self._parts is None:
            self._parts = {}
        self._part_jobs = len(self._parts)
        self._stop = False

    def get_part(self, addr):
        try:
            self._condition.acquire()
            for part_id in self._parts.keys():
                if self._parts[part_id].leader == addr \
                        and not self._parts[part_id].has_done:
                    return self._parts[part_id]
            return None
        finally:
            self._condition.release()

    def update_part_info(self, part_info, is_finished):
        try:
            self._condition.acquire()
            if self._part_jobs > 0:
                self._part_jobs -= 1
            if part_info.part_id in self._parts.keys():
                if is_finished:
                    self._parts.pop(part_info.part_id)
                else:
                    self._parts[part_info.part_id] = part_info
            self._condition.notify_all()
        finally:
            self._condition.release()

    def update_part_leader(self, part_id, leader):
        try:
            self._condition.acquire()
            if part_id in self._parts.keys():
                self._parts[part_id].leader = leader
            self._condition.notify()
        finally:
            self._condition.release()

    def is_finish(self):
        try:
            self._condition.acquire()
            if self._part_jobs == 0 or self._stop:
                return True
            else:
                return False
        finally:
            self._condition.release()

    def set_stop(self):
        logging.debug("Stop the jobs")
        try:
            self._condition.acquire()
            self._stop = True
            self._condition.notify_all()
        finally:
            self._condition.release()

    def reset_jobs(self):
        logging.debug("Reset the jobs' status ")
        try:
            self._condition.acquire()
            if self._stop:
                return 
            self._part_jobs = len(self._parts)
            self._reset_parts_status()
        finally:
            self._condition.release()

    def _reset_parts_status(self):
        for part in self._parts:
            self._parts[part].has_done = False

    def has_next(self):
        try:
            self._condition.acquire()
            if self._stop:
                return False
            return len(self._parts) != 0
        finally:
            self._condition.release()

    def wait_task(self):
        try:
            self._condition.acquire()
            self._condition.wait()
        finally:
            self._condition.release()


def do_scan_job(storage_connection,
                parts_manager,
                in_req,
                scan_vertex=True,
                partial_success=False):
    data_sets = []
    req = copy.deepcopy(in_req)
    while True:
        is_finished = False  # the part without next, it means is finished
        if parts_manager.is_finish():
            break
        part_info = parts_manager.get_part(storage_connection.storage_addr())
        if part_info is None:
            parts_manager.wait_task()
            continue
        else:
            req.part_id = part_info.part_id
            logging.debug('Scan =====> req: {}'.format(req))
            if part_info.cursor is not None:
                req.cursor = part_info.cursor
            try:
                if scan_vertex:
                    resp = storage_connection.scan_vertex(req)
                else:
                    resp = storage_connection.scan_edge(req)
                logging.debug('Scan <==== get resp: {}'.format(resp))
                if len(resp.result.failed_parts) != 0:
                    if resp.result.failed_parts[0].code == ErrorCode.E_LEADER_CHANGED:
                        if resp.result.failed_parts[0].leader is None:
                            logging.error('Happen leader change, but the leader is None')
                            raise RuntimeError('Happen leader change, but the leader is None')
                        parts_manager.update_part_leader(resp.result.failed_parts[0].part_id,
                                                         resp.result.failed_parts[0].leader)
                        logging.warning('part_id {} has leader change, '
                                        'old leader is {}, new leader is {}'
                                        .format(part_info.part_id, storage_connection.storage_addr(),
                                                resp.result.failed_parts[0].leader))
                        storage_connection.update_leader_info(req.space_id,
                                                              req.part_id,
                                                              resp.result.failed_parts[0].leader)
                        continue
                    error = 'Query storage: {}, part id: {} failed: {}' \
                        .format(storage_connection.storage_addr(),
                                part_info.part_id, resp.result.failed_parts[0].code)
                    if not partial_success:
                        logging.error(error)
                        parts_manager.set_stop()
                        return error, []
                    logging.error(error)
                    is_finished = True
                    continue
                part_info.has_done = True
                if resp.has_next:
                    part_info.cursor = resp.next_cursor
                    logging.debug("Get next next_cursor: {}".format(resp.next_cursor))
                else:
                    is_finished = True
                if scan_vertex:
                    logging.debug("resp.vertex_data size: {}".format(len(resp.vertex_data.rows)))
                    if len(resp.vertex_data.column_names) == 0:
                        return 'Part id: {} return empty column names'.format(part_info.part_id)
                    if len(resp.vertex_data.rows) == 0:
                        continue
                    data_sets.append(resp.vertex_data)
                else:
                    logging.debug("resp.edge_data size: {}".format(len(resp.edge_data.rows)))
                    if len(resp.edge_data.column_names) == 0:
                        return 'Part id: {} return empty column names'.format(part_info.part_id)
                    if len(resp.edge_data.rows) == 0:
                        continue
                    data_sets.append(resp.edge_data)
            except Exception as e:
                import traceback
                logging.error(traceback.format_exc())
                parts_manager.set_stop()
                return str(e), None
            finally:
                parts_manager.update_part_info(part_info, is_finished)
    return None, data_sets
