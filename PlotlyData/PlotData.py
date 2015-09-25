#!/usr/bin/env python2.7
'''
Created on Sep 22, 2015

@author: shaunz
'''
from PlotlyAPI import PlotlyAPI
import sys

for path in sys.path:
    print path
'''
py = PlotlyAPI()
exp_list = [
        'y1:customs_export_usd_SITC_primary_food+customs_export_usd_SITC_primary_yanjiu;food related export',
        'y2:customs_import_usd_SITC_primary_food+customs_import_usd_SITC_primary_yanjiu;food related import',
        ]

py._create_ts(exp_list,'Food','lines')
'''