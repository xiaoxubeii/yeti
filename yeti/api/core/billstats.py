#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.api import BaseResource
from flask import request
from yeti.service.bill import Bill as BillServ


class BillStats(BaseResource):
    RESOURCE_NAME = 'billstats'

    def post(self, context):
        """
        @api {post} /api/billstats 查询账单
        @apiName StatsBill
        @apiGroup BillStats
        @apiExample {curl} 案例:
            curl -X POST -i http://localhost/api/billstats -d
            "
                {
                    'aggregate': 'data',
                    'consolidate': 'daily',
                    'start': '2017-01-01 12AM',
                    'end': '2017-07-20 01AM',
                    'group_by': 'tag:BU',
                    'filter': {
                        'tag': {
                            'BU': 'dtu'
                        }
                    }
                }

           "

        @apiParam {String} start 开始时间，格式为2017-01-01 01PM
        @apiParam {String} end 结束时间，格式同上
        @apiParam {String} group_by 以什么条件分组，可选值为resource_type、usage_type、operation、自定义tag、pool_id、region_id。其中自定义tag关键词需带"tag:"前缀。
        @apiParam {String} consolidate 合计单位，可选值为hourly、daily、yearly
        @apiParam {String} aggregate 聚合单位，可选值为data、stats
        @apiParam {String} filter 过滤条件，可选值为resource_type、usage_type、operation、自定义tag、region_id、pool_id
        """
        json = request.get_json()
        bs = BillServ()
        return bs.get_data(**json)
