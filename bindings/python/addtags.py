#!/usr/bin/env python3
# Dependencies:
# - auditwheel>=3,<4
# Usage:
# $ python addtags.py WHEEL_PATH TARGET_PLATFORM WHEEL_DIR
import os
from os.path import isfile, exists, basename, join as pjoin
from sys import argv
from auditwheel.policy import get_priority_by_name, get_replace_platforms
from auditwheel.wheel_abi import analyze_wheel_abi
from auditwheel.wheeltools import InWheelCtx, add_platforms


WHEEL_PATH, TARGET_PLATFORM, WHEEL_DIR = argv[1], argv[2], argv[3]


def repair_wheel(wheel_path, abi, wheel_dir):
    wheel_fname = basename(wheel_path)
    with InWheelCtx(wheel_path) as ctx:
        ctx.out_wheel = pjoin(wheel_dir, wheel_fname)
        ctx.out_wheel = add_platforms(ctx, [abi],
                                      get_replace_platforms(abi))
    return ctx.out_wheel


def main(wheel_path=WHEEL_PATH, abi=TARGET_PLATFORM, wheel_dir=WHEEL_DIR):
    if not isfile(wheel_path):
        raise FileNotFoundError('cannot access wheel file %s' % (wheel_path,))

    if not exists(wheel_dir):
        os.makedirs(wheel_dir)

    reqd_tag = get_priority_by_name(abi)
    out_wheel = repair_wheel(wheel_path, abi, wheel_dir)

    if out_wheel is not None:
        analyzed_tag = analyze_wheel_abi(out_wheel).overall_tag
        if reqd_tag < get_priority_by_name(analyzed_tag):
            print('Wheel is eligible for a higher priority tag. '
                  'You requested %s but I have found this wheel is '
                  'eligible for %s.' % (abi, analyzed_tag))
            out_wheel = repair_wheel(wheel_path, analyzed_tag, wheel_dir)

        print('Fixed-up wheel written to %s' % (out_wheel,))
